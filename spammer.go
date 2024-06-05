package main

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	outChans := make([]chan interface{}, 0, len(cmds)+1)
	for i := range len(cmds) + 1 {
		if i == 0 {
			outChans = append(outChans, nil)
			continue
		}

		outChans = append(outChans, make(chan interface{}))
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(cmds))

	for i := len(cmds); i >= 1; i-- {
		go func(counter int) {
			cmds[counter-1](outChans[counter-1], outChans[counter])
			defer func() {
				wg.Done()
				close(outChans[counter])
			}()
		}(i)
	}

	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	users := &sync.Map{}
	wg := &sync.WaitGroup{}

	for emailData := range in {
		email := emailData.(string)
		wg.Add(1)

		go func() {
			defer wg.Done()
			user := GetUser(email)
			_, loaded := users.LoadOrStore(user.ID, struct{}{})
			if !loaded {
				out <- user
			}
		}()
	}

	wg.Wait()
}

func selectMessagesWorker(wg *sync.WaitGroup, u []User, out chan<- interface{}) {
	defer wg.Done()
	res, _ := GetMessages(u...)
	for _, msg := range res {
		out <- msg
	}
}

func SelectMessages(in, out chan interface{}) {
	users := make([]User, 0, GetMessagesMaxUsersBatch)
	wg := &sync.WaitGroup{}

	for userData := range in {
		user := userData.(User)
		users = append(users, user)

		if len(users) == GetMessagesMaxUsersBatch {
			wg.Add(1)
			go selectMessagesWorker(wg, users, out)

			users = make([]User, 0, GetMessagesMaxUsersBatch)
		}
	}

	if len(users) > 0 {
		wg.Add(1)
		go selectMessagesWorker(wg, users, out)
	}

	wg.Wait()
}

func checkSpamWorker(wg *sync.WaitGroup, in, out chan interface{}) {
	defer wg.Done()

	for msgIDData := range in {
		msgID := msgIDData.(MsgID)

		res, _ := HasSpam(msgID)
		runtime.Gosched()

		out <- MsgData{
			ID:      msgID,
			HasSpam: res,
		}
	}
}

func CheckSpam(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	wg.Add(HasSpamMaxAsyncRequests)

	for range HasSpamMaxAsyncRequests {
		go checkSpamWorker(wg, in, out)
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	results := make([]MsgData, 0)
	for msgDataUntyped := range in {
		results = append(results, msgDataUntyped.(MsgData))
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].HasSpam != results[j].HasSpam {
			return results[i].HasSpam
		}

		return results[i].ID < results[j].ID
	})

	for _, result := range results {
		out <- fmt.Sprintf("%t %d", result.HasSpam, result.ID)
	}
}
