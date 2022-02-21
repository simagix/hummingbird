// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"time"

	"github.com/simagix/gox"
)

// ConfigCopier copies configuration including indexes from source to target
func ConfigCopier() error {
	now := time.Now()
	logger := gox.GetLogger("")
	logger.Remark("copying configurations")
	err := IndexCopier()
	if err != nil {
		return err
	}
	logger.Infof("configurations copied, took %v", time.Since(now))
	return nil
}
