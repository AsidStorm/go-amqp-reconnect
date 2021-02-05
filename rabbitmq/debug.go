package rabbitmq

import "log"

func debugf(format string, args ...interface{}) {
	if !Debug {
		return
	}

	log.Printf(format, args...)
}
