package utils

import "math"

func BuildIntervalFromStartAndEndRoundedToInterval(N, M, interval int64) []int64 {
	var numberList []int64
	current := N

	for current <= M {
		numberList = append(numberList, RoundToNearest(current, interval))
		current += interval
	}

	return numberList
}

func RoundToNearest(number, interval int64) int64 {
	return int64(math.Round(float64(number)/float64(interval))) * interval
}
