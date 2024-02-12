package dml

import "go-dbms/services/executor/parent"

type DML struct {
	*parent.ExecutorService
}

func New(es *parent.ExecutorService) *DML {
	return &DML{ExecutorService: es}
}
