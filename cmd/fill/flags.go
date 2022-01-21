package main

import (
	"fmt"
	"strconv"
)

type IntsFlag []int

func (i *IntsFlag) String() string {
	return fmt.Sprintf("%v", *i)
}

func (i *IntsFlag) Set(value string) error {
	num, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	*i = append(*i, num)
	return nil
}
