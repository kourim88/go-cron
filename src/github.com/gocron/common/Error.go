package common

import "errors"

var (
	ERR_LOCK_ALREADY_REQUIRED = errors.New("ロックは占有されています。")
	ERR_NO_LOCAL_IP_FOUND = errors.New("IPが見つかりません。")
)
