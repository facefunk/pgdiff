package pgdiff

var (
	_ Stringer = Line("")
	_ Stringer = Notice("")
	_ Stringer = Error("")
	_ error    = Error("")
)
