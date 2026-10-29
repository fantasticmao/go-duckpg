package duckpg

import (
	"regexp"
	"strings"
)

const pgVersionText = "PostgreSQL 15.0 (DuckDB 2.0.0)"

// compatKind is how a whole client statement is handled before DuckDB sees it.
type compatKind int

const (
	compatPass compatKind = iota
	compatLocal
	compatRewrite
)

// compatEffect is applied when the statement executes, not when it is parsed.
type compatEffect struct {
	sets   map[string]string
	resets []string
}

type compatClass struct {
	kind   compatKind
	sql    string
	tag    string
	effect compatEffect
}

// startupSettings are the values advertised at connection startup, plus defaults
// for PostgreSQL GUCs that DuckDB 2.0 does not implement.
var startupSettings = map[string]string{
	"server_version":              "15.0",
	"server_encoding":             "UTF8",
	"client_encoding":             "UTF8",
	"datestyle":                   "ISO, MDY",
	"timezone":                    "UTC",
	"integer_datetimes":           "on",
	"standard_conforming_strings": "on",
	"extra_float_digits":          "1",
	"application_name":            "",
	"intervalstyle":               "postgres",
	"bytea_output":                "hex",
	"transaction_isolation":       "read committed",
}

// settableGUCs are PostgreSQL session parameters DuckDB 2.0 rejects.
// DuckDB settings such as search_path are not listed and run as written.
var settableGUCs = map[string]struct{}{
	"extra_float_digits":          {},
	"application_name":            {},
	"client_encoding":             {},
	"standard_conforming_strings": {},
	"datestyle":                   {},
	"timezone":                    {},
	"intervalstyle":               {},
	"bytea_output":                {},
}

var (
	reSet            = regexp.MustCompile(`(?i)^SET\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:=|TO)\s*(.+)$`)
	reReset          = regexp.MustCompile(`(?i)^RESET\s+([A-Za-z_][A-Za-z0-9_]*)$`)
	reShow           = regexp.MustCompile(`(?i)^SHOW\s+([A-Za-z_][A-Za-z0-9_]*)$`)
	reVersion        = regexp.MustCompile(`(?i)^SELECT\s+(?:pg_catalog\.)?version\s*\(\s*\)$`)
	reCurrentSetting = regexp.MustCompile(`(?i)^SELECT\s+(?:pg_catalog\.)?current_setting\s*\(\s*'((?:[^']|'')*)'\s*(?:,\s*(true|false))?\s*\)$`)
	reSetConfig      = regexp.MustCompile(`(?i)^SELECT\s+(?:pg_catalog\.)?set_config\s*\(\s*'((?:[^']|'')*)'\s*,\s*'((?:[^']|'')*)'\s*,\s*(true|false)\s*\)$`)
)

func classifyCompat(query string, s *session) compatClass {
	sql := trimStatement(query)
	if sql == "" {
		return compatClass{kind: compatPass}
	}
	if class, ok := classifySet(sql); ok {
		return class
	}
	if class, ok := classifyReset(sql); ok {
		return class
	}
	if class, ok := classifyShow(sql, s); ok {
		return class
	}
	if reVersion.MatchString(sql) {
		return compatClass{
			kind: compatRewrite,
			sql:  "SELECT " + sqlStringLiteral(pgVersionText) + " AS version",
		}
	}
	if class, ok := classifyCurrentSetting(sql, s); ok {
		return class
	}
	if class, ok := classifySetConfig(sql); ok {
		return class
	}
	return compatClass{kind: compatPass}
}

func classifySet(sql string) (compatClass, bool) {
	m := reSet.FindStringSubmatch(sql)
	if m == nil {
		return compatClass{}, false
	}
	name := strings.ToLower(m[1])
	if _, ok := settableGUCs[name]; !ok {
		return compatClass{}, false
	}
	raw := strings.TrimSpace(m[2])
	if strings.Contains(raw, ";") {
		return compatClass{}, false
	}
	if quoted, ok := unquotePGString(raw); ok {
		return compatClass{
			kind:   compatLocal,
			tag:    "SET",
			effect: compatEffect{sets: map[string]string{name: quoted}},
		}, true
	}
	if strings.EqualFold(raw, "DEFAULT") {
		return compatClass{
			kind:   compatLocal,
			tag:    "SET",
			effect: compatEffect{resets: []string{name}},
		}, true
	}
	return compatClass{
		kind:   compatLocal,
		tag:    "SET",
		effect: compatEffect{sets: map[string]string{name: raw}},
	}, true
}

func classifyReset(sql string) (compatClass, bool) {
	m := reReset.FindStringSubmatch(sql)
	if m == nil {
		return compatClass{}, false
	}
	name := strings.ToLower(m[1])
	if _, ok := settableGUCs[name]; !ok {
		return compatClass{}, false
	}
	return compatClass{
		kind:   compatLocal,
		tag:    "RESET",
		effect: compatEffect{resets: []string{name}},
	}, true
}

func classifyShow(sql string, s *session) (compatClass, bool) {
	m := reShow.FindStringSubmatch(sql)
	if m == nil {
		return compatClass{}, false
	}
	name := strings.ToLower(m[1])
	if _, ok := settableGUCs[name]; !ok && name != "transaction_isolation" {
		return compatClass{}, false
	}
	value, _ := lookupSetting(s, name)
	return compatClass{
		kind: compatRewrite,
		sql:  "SELECT " + sqlStringLiteral(value) + " AS " + name,
	}, true
}

func classifyCurrentSetting(sql string, s *session) (compatClass, bool) {
	m := reCurrentSetting.FindStringSubmatch(sql)
	if m == nil {
		return compatClass{}, false
	}
	name := strings.ToLower(unescapePGString(m[1]))
	missingOK := strings.EqualFold(m[2], "true")
	value, ok := lookupSetting(s, name)
	if !ok {
		if missingOK {
			return compatClass{
				kind: compatRewrite,
				sql:  "SELECT CAST(NULL AS VARCHAR) AS current_setting",
			}, true
		}
		return compatClass{}, false
	}
	return compatClass{
		kind: compatRewrite,
		sql:  "SELECT " + sqlStringLiteral(value) + " AS current_setting",
	}, true
}

func classifySetConfig(sql string) (compatClass, bool) {
	m := reSetConfig.FindStringSubmatch(sql)
	if m == nil {
		return compatClass{}, false
	}
	name := strings.ToLower(unescapePGString(m[1]))
	if _, ok := settableGUCs[name]; !ok {
		return compatClass{}, false
	}
	value := unescapePGString(m[2])
	return compatClass{
		kind:   compatRewrite,
		sql:    "SELECT " + sqlStringLiteral(value) + " AS set_config",
		effect: compatEffect{sets: map[string]string{name: value}},
	}, true
}

func lookupSetting(s *session, name string) (string, bool) {
	if s != nil && s.settings != nil {
		if v, ok := s.settings[name]; ok {
			return v, true
		}
	}
	v, ok := startupSettings[name]
	return v, ok
}

func (s *session) applyEffect(e compatEffect) {
	for _, name := range e.resets {
		delete(s.settings, name)
	}
	for name, value := range e.sets {
		s.settings[name] = value
	}
}

func trimStatement(query string) string {
	s := strings.TrimSpace(query)
	for strings.HasSuffix(s, ";") {
		s = strings.TrimSpace(strings.TrimSuffix(s, ";"))
	}
	return s
}

func unquotePGString(s string) (string, bool) {
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return "", false
	}
	return unescapePGString(s[1 : len(s)-1]), true
}

func unescapePGString(s string) string {
	return strings.ReplaceAll(s, "''", "'")
}

func sqlStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
