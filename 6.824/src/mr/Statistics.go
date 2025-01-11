package mr

type StatisticInfo struct {
	MapCrash    int //the number of Crash in the whole stage of Map
	ReduceCrash int //the number of Crash in the whole stage of Reduce
}
