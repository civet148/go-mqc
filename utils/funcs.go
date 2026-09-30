package utils

func BlockRoutine() {
	var ch = make(chan bool)
	<-ch
}
