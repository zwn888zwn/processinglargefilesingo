package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/mmap"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func Test(t *testing.T) {
	tableTests := []struct {
		file     string
		expected result
	}{
		{
			file: "./data/itcont_sample_40.txt",
			expected: result{
				numRows:           40,
				peopleCount:       35,
				commonName:        "LAURA",
				commonNameCount:   4,
				donationMonthFreq: map[string]int{"01": 7, "02": 2, "03": 6, "04": 2, "05": 3, "06": 2, "07": 1, "08": 2, "11": 15},
			},
		},
		{
			file: "./data/itcont_sample_4000.txt",
			expected: result{
				numRows:           4000,
				peopleCount:       35,
				commonName:        "LAURA",
				commonNameCount:   400,
				donationMonthFreq: map[string]int{"01": 700, "02": 200, "03": 600, "04": 200, "05": 300, "06": 200, "07": 100, "08": 200, "11": 1500},
			},
		},
	}

	for _, tt := range tableTests {
		require.Equal(t, tt.expected, sequential(tt.file))
		require.Equal(t, tt.expected, concurrent(tt.file, 2, 10))
	}
}

func Benchmark(b *testing.B) {
	tableBenchmarks := []struct {
		name    string
		file    string
		inputs  [][]int
		benchFn func(file string, numWorkers, batchSize int) result
	}{
		{
			name:   "Sequential",
			file:   "./data/itcont.txt",
			inputs: [][]int{{0, 0}},
			benchFn: func(file string, numWorkers, batchSize int) result {
				return sequential(file)
			},
		},
		//{
		//	name:   "Concurrent",
		//	file:   "./data/itcont.txt",
		//	inputs: [][]int{{1, 1}, {1, 1000}, {10, 1000}, {10, 10000}, {10, 100000}},
		//	benchFn: func(file string, numWorkers, batchSize int) result {
		//		return concurrent(file, numWorkers, batchSize)
		//	},
		//},
	}

	for _, tb := range tableBenchmarks {
		for _, x := range tb.inputs {
			numWorkers := x[0]
			batchSize := x[1]

			bName := fmt.Sprintf("%s %03d workers %04d batchSize", tb.name, numWorkers, batchSize)
			b.Run(bName, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					tb.benchFn(tb.file, numWorkers, batchSize)
				}
			})
		}
	}
}

type result struct {
	numRows           int
	peopleCount       int
	commonName        string
	commonNameCount   int
	donationMonthFreq map[string]int
}

// processRow takes a pipe-separated line and returns the firstName, fullName, and month.
// this function was created to be somewhat compute intensive and not accurate.
func processRow(text string) (firstName, fullName, month string) {
	row := strings.Split(text, "|")

	// extract full name
	fullName = strings.Replace(strings.TrimSpace(row[7]), " ", "", -1)

	// extract first name
	name := strings.TrimSpace(row[7])
	if name != "" {
		startOfName := strings.Index(name, ", ") + 2
		if endOfName := strings.Index(name[startOfName:], " "); endOfName < 0 {
			firstName = name[startOfName:]
		} else {
			firstName = name[startOfName : startOfName+endOfName]
		}
		if strings.HasSuffix(firstName, ",") {
			firstName = strings.Replace(firstName, ",", "", -1)
		}
	}

	// extract month
	date := strings.TrimSpace(row[13])
	if len(date) == 8 {
		month = date[:2]
	} else {
		month = "--"
	}

	return firstName, fullName, month
}

// sequential processes a file line by line using processRow.
func sequential(file string) result {
	go http.ListenAndServe("0.0.0.0:6060", nil)
	return mmapReadV3(file)
	//return mmapReadV2(file)
	//return mmapRead(file)
	//return bufioBigBuf(file)
	//return bufioScan(file)
	//return readFullFile(file)
}

func readFullFile(file string) result {
	res := result{donationMonthFreq: map[string]int{}}

	// track full names
	fullNamesRegister := make(map[string]bool)

	// track first name frequency
	firstNameMap := make(map[string]int)

	// 读取整个文件内容
	content, err := os.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}

	// 将文件内容按行分割成字符串数组
	lines := strings.Split(string(content), "\n")

	// 遍历字符串数组，输出每一行内容
	for _, row := range lines {
		if row == "" {
			continue
		}
		firstName, fullName, month := processRow(row)

		// add fullname
		fullNamesRegister[fullName] = true

		// update common firstName
		firstNameMap[firstName]++
		if firstNameMap[firstName] > res.commonNameCount {
			res.commonName = firstName
			res.commonNameCount = firstNameMap[firstName]
		}
		// add month freq
		res.donationMonthFreq[month]++
		// update numRows
		res.numRows++
		res.peopleCount = len(fullNamesRegister)
	}

	return res
}

func bufioScan(file string) result {
	res := result{donationMonthFreq: map[string]int{}}

	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	// track full names
	fullNamesRegister := make(map[string]bool)

	// track first name frequency
	firstNameMap := make(map[string]int)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		row := scanner.Text()
		firstName, fullName, month := processRow(row)

		// add fullname
		fullNamesRegister[fullName] = true

		// update common firstName
		firstNameMap[firstName]++
		if firstNameMap[firstName] > res.commonNameCount {
			res.commonName = firstName
			res.commonNameCount = firstNameMap[firstName]
		}
		// add month freq
		res.donationMonthFreq[month]++
		// update numRows
		res.numRows++
		res.peopleCount = len(fullNamesRegister)
	}

	return res
}

func mmapRead(file string) result {
	res := result{donationMonthFreq: map[string]int{}}
	// track full names
	fullNamesRegister := make(map[string]bool)
	// track first name frequency
	firstNameMap := make(map[string]int)

	// Provide the filename & let mmap do the magic
	r, err := mmap.Open(file)
	if err != nil {
		panic(err)
	}

	noLastLoop := true
	begin := 0

	for begin < r.Len() && noLastLoop {
		p := make([]byte, 64*1024*1024)

		//Read the file in memory at an offset of 0 => the entire thing
		_, err = r.ReadAt(p, int64(begin))
		if err != nil {
			if err == io.EOF {
				noLastLoop = false
			} else {
				panic(err)
			}
		}

		// 将文件内容按行分割成字符串数组
		lines := strings.Split(string(p), "\n")

		// 遍历字符串数组，输出每一行内容
		for i, row := range lines {
			if i == len(lines)-1 {
				if row != "" {
					begin = begin + len(p) - len(row)
				}
				continue
			}
			firstName, fullName, month := processRow(row)

			// add fullname
			fullNamesRegister[fullName] = true

			// update common firstName
			firstNameMap[firstName]++
			if firstNameMap[firstName] > res.commonNameCount {
				res.commonName = firstName
				res.commonNameCount = firstNameMap[firstName]
			}
			// add month freq
			res.donationMonthFreq[month]++
			// update numRows
			res.numRows++
			res.peopleCount = len(fullNamesRegister)
		}
	}

	return res
}

func mmapReadV2(file string) result {
	res := result{donationMonthFreq: map[string]int{}}
	// track full names
	fullNamesRegister := make(map[string]bool)
	// track first name frequency
	firstNameMap := make(map[string]int)

	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		panic(err)
	}

	size := fi.Size()

	noLastLoop := true
	begin := 0
	pageSize := 16 * 1024
	blockSize := 1 * pageSize
	readLen := blockSize
	lastStr := ""

	for begin*blockSize < int(size) && noLastLoop {
		if begin*blockSize+readLen > int(size) {
			noLastLoop = false
			readLen = int(size) - begin*blockSize
		}

		data, err := syscall.Mmap(int(f.Fd()), int64(begin*blockSize), readLen, syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			panic(err)
		}

		// 将文件内容按行分割成字符串数组
		lines := strings.Split(lastStr+string(data), "\n")

		// 遍历字符串数组，输出每一行内容
		for i, row := range lines {
			if i == len(lines)-1 {
				if row != "" {
					lastStr = row
				}
				continue
			}
			firstName, fullName, month := processRow(row)

			// add fullname
			fullNamesRegister[fullName] = true

			// update common firstName
			firstNameMap[firstName]++
			if firstNameMap[firstName] > res.commonNameCount {
				res.commonName = firstName
				res.commonNameCount = firstNameMap[firstName]
			}
			// add month freq
			res.donationMonthFreq[month]++
			// update numRows
			res.numRows++
			res.peopleCount = len(fullNamesRegister)
		}

		begin++
		syscall.Munmap(data)
	}

	return res
}

func mmapReadV3(file string) result {
	res := result{donationMonthFreq: map[string]int{}}
	// track full names
	fullNamesRegister := make(map[string]bool)
	// track first name frequency
	firstNameMap := make(map[string]int)

	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		panic(err)
	}

	size := fi.Size()

	noLastLoop := true
	begin := 0
	pageSize := 16 * 1024
	blockSize := 10 * 1024 * pageSize
	readLen := blockSize
	lastStr := ""

	for begin*blockSize < int(size) && noLastLoop {
		if begin*blockSize+readLen > int(size) {
			noLastLoop = false
			readLen = int(size) - begin*blockSize
		}

		data, err := syscall.Mmap(int(f.Fd()), int64(begin*blockSize), readLen, syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			panic(err)
		}

		lastIndex := 0
		index := 0
		for index != -1 {
			if index = bytes.IndexByte(data[lastIndex:], '\n'); index >= 0 {
				// We have a full newline-terminated line.
				row := string(data[lastIndex : lastIndex+index])
				if lastIndex == 0 {
					row = lastStr + row
				}

				firstName, fullName, month := processRow(row)

				// add fullname
				fullNamesRegister[fullName] = true

				// update common firstName
				firstNameMap[firstName]++
				if firstNameMap[firstName] > res.commonNameCount {
					res.commonName = firstName
					res.commonNameCount = firstNameMap[firstName]
				}
				// add month freq
				res.donationMonthFreq[month]++
				// update numRows
				res.numRows++
				res.peopleCount = len(fullNamesRegister)

				lastIndex = index + lastIndex + 1
			} else {
				lastStr = string(data[lastIndex:])
				break
			}

		}

		begin++
		syscall.Munmap(data)
	}

	return res
}

func bufioBigBuf(file string) result {
	res := result{donationMonthFreq: map[string]int{}}

	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	// track full names
	fullNamesRegister := make(map[string]bool)

	// track first name frequency
	firstNameMap := make(map[string]int)

	scanner := bufio.NewScanner(f)

	const maxCapacity = 32 * 1024 * 1024 // 例如，1MB，可读取任何 1MB 的行。
	buf := make([]byte, maxCapacity)     // 初始缓冲大小 1MB，无需多次扩容
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		row := scanner.Text()
		firstName, fullName, month := processRow(row)

		// add fullname
		fullNamesRegister[fullName] = true

		// update common firstName
		firstNameMap[firstName]++
		if firstNameMap[firstName] > res.commonNameCount {
			res.commonName = firstName
			res.commonNameCount = firstNameMap[firstName]
		}
		// add month freq
		res.donationMonthFreq[month]++
		// update numRows
		res.numRows++
		res.peopleCount = len(fullNamesRegister)
	}

	return res
}

// concurrent processes a file by splitting the file
// processing the files concurrently and returning the result.
func concurrent(file string, numWorkers, batchSize int) (res result) {
	res = result{donationMonthFreq: map[string]int{}}

	type processed struct {
		numRows    int
		fullNames  []string
		firstNames []string
		months     []string
	}

	// open file
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	// reader creates and returns a channel that recieves
	// batches of rows (of length batchSize) from the file
	reader := func(ctx context.Context, rowsBatch *[]string) <-chan []string { //返回只可以用来接收 []string 类型数据的chan
		out := make(chan []string)

		scanner := bufio.NewScanner(f)

		go func() {
			// 如果channel c已经被关闭,继续往它发送数据会导致panic: send on closed channel
			// 但是从这个关闭的channel中不但可以读取出已发送的数据，还可以不断的读取零值
			defer close(out) // close channel when we are done sending all rows

			for {
				scanned := scanner.Scan()

				select {
				case <-ctx.Done():
					return
				default:
					row := scanner.Text()
					// if batch size is complete or end of file, send batch out
					if len(*rowsBatch) == batchSize || !scanned {
						out <- *rowsBatch
						*rowsBatch = []string{} // clear batch
					}
					*rowsBatch = append(*rowsBatch, row) // add row to current batch
				}

				// if nothing else to scan return
				if !scanned {
					return
				}
			}
		}()

		return out
	}

	// worker takes in a read-only channel to recieve batches of rows.
	// After it processes each row-batch it sends out the processed output
	// on its channel.
	worker := func(ctx context.Context, rowBatch <-chan []string) <-chan processed {
		out := make(chan processed)

		go func() {
			defer close(out)

			p := processed{}
			for rowBatch := range rowBatch {
				for _, row := range rowBatch {
					firstName, fullName, month := processRow(row)
					p.fullNames = append(p.fullNames, fullName)
					p.firstNames = append(p.firstNames, firstName)
					p.months = append(p.months, month)
					p.numRows++
				}
			}
			out <- p
		}()

		return out
	}

	// combiner takes in multiple read-only channels that receive processed output
	// (from workers) and sends it out on it's own channel via a multiplexer.
	combiner := func(ctx context.Context, inputs ...<-chan processed) <-chan processed {
		out := make(chan processed)

		var wg sync.WaitGroup
		multiplexer := func(p <-chan processed) {
			defer wg.Done()

			for in := range p { // 如果通过range读取，channel关闭后for循环会跳出
				select {
				case <-ctx.Done():
				case out <- in:
				}
			}
		}

		// add length of input channels to be consumed by mutiplexer
		wg.Add(len(inputs))
		for _, in := range inputs {
			go multiplexer(in)
		}

		// close channel after all inputs channels are closed
		go func() {
			wg.Wait()
			close(out)
		}()

		return out
	}

	// create a main context, and call cancel at the end, to ensure all our
	// goroutines exit without leaving leaks.
	// Particularly, if this function becomes part of a program with
	// a longer lifetime than this function.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// STAGE 1: start reader
	rowsBatch := []string{}
	rowsCh := reader(ctx, &rowsBatch)

	// STAGE 2: create a slice of processed output channels with size of numWorkers
	// and assign each slot with the out channel from each worker.
	workersCh := make([]<-chan processed, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workersCh[i] = worker(ctx, rowsCh)
	}

	firstNameCount := map[string]int{}
	fullNameCount := map[string]bool{}

	// STAGE 3: read from the combined channel and calculate the final result.
	// this will end once all channels from workers are closed!
	for processed := range combiner(ctx, workersCh...) {
		// add number of rows processed by worker
		res.numRows += processed.numRows

		// add months processed by worker
		for _, month := range processed.months {
			res.donationMonthFreq[month]++
		}

		// use full names to count people
		for _, fullName := range processed.fullNames {
			fullNameCount[fullName] = true
		}
		res.peopleCount = len(fullNameCount)

		// update most common first name based on processed results
		for _, firstName := range processed.firstNames {
			firstNameCount[firstName]++

			if firstNameCount[firstName] > res.commonNameCount {
				res.commonName = firstName
				res.commonNameCount = firstNameCount[firstName]
			}
		}
	}

	return res
}
