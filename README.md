Простая реализация key-value хранилища на Go. Поддерживает базовые команды Redis и многопоточное обслуживание клиентов с использованием протокола RESP.

Команды: PING, ECHO, SET, GET, RPUSH, LPUSH, LRANGE, LPOP, BLPOP, TYPE. Конкурентная обработка запросов (goroutines, мьютексы). Поддержка протокола RESP

Можно подключиться через redis-cli: redis-cli -h localhost -p 6379.



