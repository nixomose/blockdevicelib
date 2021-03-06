// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package blockdevicelib

type Lbd_config struct {
	Log     logfields
	Zosbd2  zosbd2fields
	Catalog catalogfields
}
type logfields struct {
	Log_file  string
	Log_level int
}
type catalogfields struct {
	Catalog_file string
}
type zosbd2fields struct {
	Control_device string
}
