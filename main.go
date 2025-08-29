package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

/* Заметки на работу:
Сохранение в файле
Вынос общего и улучшение кода
Улучшение BLPOP
*/

var (
	store    = make(map[string]string)        // Глобальная мапа для SET
	mapSlice = make(map[string][]string)      // Глобальная мапа для списков
	waiting  = make(map[string][]chan string) // Глобальная мапа для BLPOP
	// streams  = make(map[string][]map[string]string) Глобальная мапа для потоков
	mu sync.RWMutex // Мьютекс для синхронизации
)

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Парсер команд
	for {
		text, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Ошибка чтения: ", err)
			return
		}

		text = strings.TrimRight(text, "\r\n")
		var arr []string
		if text[0] == '*' {
			length, err := strconv.Atoi(text[1:])
			if err != nil {
				fmt.Println("Ошибка конвертации: ", err)
				return
			}
			arr = make([]string, 0, length)

			for range length {
				line, err := reader.ReadString('\n')
				if err != nil {
					fmt.Println("Ошибка чтения: ", err)
					return
				}

				line = strings.TrimRight(line, "\r\n")
				if line[0] != '$' {
					fmt.Println("Первый символ не $")
					return
				}

				lineLength, err := strconv.Atoi(line[1:])
				if err != nil {
					fmt.Println("Ошибка конвертации: ", err)
					return
				}

				elem, err := reader.ReadString('\n')
				if err != nil {
					fmt.Println("Ошибка конвертации: ", err)
					return
				}
				elem = strings.TrimRight(elem, "\r\n")

				if len(elem) != lineLength {
					fmt.Printf("Неккоректная длина аргумента: длина %s != %d\n", elem, lineLength)
					return
				}

				arr = append(arr, elem)
			}
		} else {
			fmt.Println("Первый символ не *")
			return
		}

		// Выполняем запросы
		switch strings.ToUpper(arr[0]) {
		case "ECHO":
			res := fmt.Sprintf("+%s\r\n", arr[1])
			if _, err := conn.Write([]byte(res)); err != nil {
				fmt.Println("Ошибка записи: ", err)
				return
			}
		case "PING":
			if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
				fmt.Println("Ошибка записи: ", err)
				return
			}
		case "SET":
			mu.Lock()
			store[arr[1]] = arr[2]

			if len(arr) > 3 && strings.ToUpper(arr[3]) == "PX" {
				timer, err := strconv.Atoi(arr[4])
				if err != nil {
					mu.Unlock()
					fmt.Println("Ошибка парсинга: ", err)
					return
				}
				go sleepSetter(arr[1], timer)
			}
			mu.Unlock()

			if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
				fmt.Println("Ошибка записи: ", err)
				return
			}
		case "GET":
			mu.RLock()
			elem, ok := store[arr[1]]
			mu.RUnlock()

			if ok {
				if _, err := conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(elem), elem)); err != nil {
					fmt.Println("Ошибка записи: ", err)
					return
				}
			} else {
				if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
					fmt.Println("Ошибка записи: ", err)
					return
				}
			}
		case "RPUSH":
			mu.Lock()
			_, ok := mapSlice[arr[1]]
			if !ok {
				mapSlice[arr[1]] = make([]string, 0)
			}

			// Флаг для BLPOP
			flag := 0
			if len(waiting[arr[1]]) > 0 {
				ch := waiting[arr[1]][0]
				waiting[arr[1]] = waiting[arr[1]][1:]
				ch <- arr[2]
				flag += 1
			}

			mapSlice[arr[1]] = append(mapSlice[arr[1]], arr[2+flag:]...)
			length := len(mapSlice[arr[1]]) + flag
			mu.Unlock()

			if _, err := conn.Write(fmt.Appendf(nil, ":%d\r\n", length)); err != nil {
				fmt.Println("Ошибка записи: ", err)
				return
			}
		case "LPUSH":
			mu.Lock()
			_, ok := mapSlice[arr[1]]
			if !ok {
				mapSlice[arr[1]] = make([]string, 0)
			}

			// Флаг для BLPOP
			flag := 0
			if len(waiting[arr[1]]) > 0 {
				ch := waiting[arr[1]][0]
				waiting[arr[1]] = waiting[arr[1]][1:]
				ch <- arr[2]
				flag += 1
			}

			for i := 2 + flag; i < len(arr[2:])+2; i++ {
				mapSlice[arr[1]] = append(mapSlice[arr[1]], "")
				copy(mapSlice[arr[1]][1:], mapSlice[arr[1][0:]])
				mapSlice[arr[1]][0] = arr[i]
			}
			length := len(mapSlice[arr[1]]) + flag
			mu.Unlock()

			if _, err := conn.Write(fmt.Appendf(nil, ":%d\r\n", length)); err != nil {
				fmt.Println("Ошибка записи: ", err)
				return
			}
		case "LRANGE":
			mu.RLock()
			list, ok := mapSlice[arr[1]]
			mu.RUnlock()
			if !ok || len(list) == 0 {
				if _, err := conn.Write([]byte("*0\r\n")); err != nil {
					fmt.Println("Ошибка записи: ", err)
					return
				}
				continue
			}

			length := len(list)
			firstIndex, err := strconv.Atoi(arr[2])
			if err != nil {
				fmt.Println("Ошибка парсинга: ", err)
				return
			}
			secondIndex, err := strconv.Atoi(arr[3])
			if err != nil {
				fmt.Println("Ошибка парсинга: ", err)
				return
			}

			if firstIndex < 0 {
				firstIndex = length + firstIndex
			}
			if secondIndex < 0 {
				secondIndex = length + secondIndex
			}

			if firstIndex >= length || firstIndex < -length {
				if _, err := conn.Write([]byte("*0\r\n")); err != nil {
					fmt.Println("Ошибка записи: ", err)
					return
				}
				continue
			}

			if firstIndex < 0 {
				firstIndex = 0
			}
			if secondIndex >= length {
				secondIndex = length - 1
			}
			if secondIndex < firstIndex {
				if _, err := conn.Write([]byte("*0\r\n")); err != nil {
					fmt.Println("Ошибка записи: ", err)
					return
				}
				continue
			}

			var b strings.Builder
			b.WriteString("*")
			b.WriteString(strconv.Itoa(secondIndex - firstIndex + 1))
			b.WriteString("\r\n")
			for i := firstIndex; i <= secondIndex; i++ {
				b.WriteString("$")
				b.WriteString(strconv.Itoa(len(list[i])))
				b.WriteString("\r\n")
				b.WriteString(list[i])
				b.WriteString("\r\n")
			}
			if _, err := conn.Write([]byte(b.String())); err != nil {
				fmt.Println("Ошибка записи: ", err)
				return
			}
		case "LLEN":
			mu.RLock()
			list, ok := mapSlice[arr[1]]
			mu.RUnlock()
			if !ok {
				if _, err := conn.Write([]byte(":0\r\n")); err != nil {
					fmt.Println("Ошибка записи: ", err)
					return
				}
				continue
			}

			length := len(list)
			if _, err := conn.Write(fmt.Appendf(nil, ":%d\r\n", length)); err != nil {
				fmt.Println("Ошибка записи: ", err)
				return
			}
		case "LPOP":
			mu.Lock()
			_, ok := mapSlice[arr[1]]
			if !ok || len(mapSlice[arr[1]]) == 0 {
				if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
					fmt.Println("Ошибка записи: ", err)
					return
				}
				continue
			}

			shift := 1
			if len(arr) > 2 {
				shift, err = strconv.Atoi(arr[2])
				if err != nil {
					fmt.Println("Ошибка парсинга: ", err)
					return
				}
				if shift > len(mapSlice[arr[1]]) {
					shift = len(mapSlice[arr[1]]) - 1
				}
			}

			result := mapSlice[arr[1]][:shift]
			mapSlice[arr[1]] = mapSlice[arr[1]][shift:]
			mu.Unlock()

			var b strings.Builder
			if shift != 1 {
				b.WriteString("*")
				b.WriteString(strconv.Itoa(len(result)))
				b.WriteString("\r\n")
			}
			for _, elem := range result {
				b.WriteString("$")
				b.WriteString(strconv.Itoa(len(elem)))
				b.WriteString("\r\n")
				b.WriteString(elem)
				b.WriteString("\r\n")
			}
			if _, err := conn.Write([]byte(b.String())); err != nil {
				fmt.Println("Ошибка записи: ", err)
				return
			}
		case "BLPOP":
			key := arr[1]
			timeout, err := strconv.ParseFloat(arr[2], 64)
			if err != nil {
				fmt.Println("Ошибка конвертации: ", err)
				return
			}

			mu.Lock()
			if len(mapSlice[key]) > 0 {
				val := mapSlice[key][0]
				mapSlice[key] = mapSlice[key][1:]
				mu.Unlock()
				res := sendMess(key, val)
				if _, err := conn.Write([]byte(res)); err != nil {
					fmt.Println("Ошибка записи: ", err)
					return
				}
				continue
			}

			ch := make(chan string, 1)
			waiting[key] = append(waiting[key], ch)
			mu.Unlock()

			if timeout == 0 {
				val := <-ch
				res := sendMess(key, val)
				if _, err := conn.Write([]byte(res)); err != nil {
					fmt.Println("Ошибка записи: ", err)
					return
				}
				continue
			} else {
				select {
				case val := <-ch:
					res := sendMess(key, val)
					if _, err := conn.Write([]byte(res)); err != nil {
						fmt.Println("Ошибка записи: ", err)
						return
					}
					continue
				case <-time.After(time.Duration(timeout * float64(time.Second))):
					if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
						fmt.Println("Ошибка записи: ", err)
						return
					}
					continue
				}
			}
		case "TYPE":
			key := arr[1]
			mu.RLock()
			defer mu.RUnlock()

			var keyType string
			if _, ok := store[key]; ok {
				keyType = "string"
			} else if _, ok := mapSlice[key]; ok {
				keyType = "list"
			} else {
				keyType = "none"
			}

			if _, err := conn.Write(fmt.Appendf(nil, "+%s\r\n", keyType)); err != nil {
				fmt.Println("Ошибка записи: ", err)
				return
			}
		default:
			if _, err := conn.Write(fmt.Appendf(nil, "-ERR unknown command '%s'\r\n", arr[0])); err != nil {
				fmt.Println("Ошибка записи: ", err)
				return
			}
		}
	}
}

func sleepSetter(key string, timer int) {
	time.Sleep(time.Duration(timer) * time.Millisecond)
	mu.Lock()
	delete(store, key)
	mu.Unlock()
}

func sendMess(key, val string) string {
	var b strings.Builder
	b.WriteString("*2\r\n$")
	b.WriteString(strconv.Itoa(len(key)))
	b.WriteString("\r\n")
	b.WriteString(key)
	b.WriteString("\r\n$")
	b.WriteString(strconv.Itoa(len(val)))
	b.WriteString("\r\n")
	b.WriteString(val)
	b.WriteString("\r\n")
	return b.String()
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}
