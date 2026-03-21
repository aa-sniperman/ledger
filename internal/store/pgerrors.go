package store

import (
	"errors"

	"github.com/jackc/pgconn"
)

const uniqueViolationCode = "23505"

func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == uniqueViolationCode
}
