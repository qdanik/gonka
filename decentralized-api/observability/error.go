package observability

import "fmt"

type ErrorFormatter struct{}

var Error = ErrorFormatter{}

func (ErrorFormatter) Fmt(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}

	message := fmt.Sprintf(format, args...)
	if message == "" {
		return err
	}

	return fmt.Errorf("%s. %w", message, err)
}