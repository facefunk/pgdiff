// Copyright (c) 2022 Facefunk. All rights reserved.
// Use of this source code is governed by the MIT license that can be found in the LICENSE file.

package pgdiff

var (
	_ Stringer = Line("")
	_ Stringer = Notice("")
	_ Stringer = Error("")
	_ error    = Error("")
)
