// Copyright (c) 2022 Facefunk. All rights reserved.
// Use of this source code is governed by the MIT license that can be found in the LICENSE file.

package pgdiff

import flag "github.com/ogier/pflag"

var (
	_ Stringer   = NewLine("")
	_ Stringer   = NewNotice("")
	_ Stringer   = NewError("")
	_ error      = NewError("")
	_ flag.Value = (*OutputSet)(nil)
)
