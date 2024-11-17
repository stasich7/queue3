## Домашнее задание от 13 нобяря 2024

Producer<br>
p - num partitions<br>
r - num replica set<br>
```
go run producer/producer.go -id=10 -p=1 -r=2
```

Consumer (Group)
```
go run consumer/consumer.go -id=10
```


## Проблемы с пониманием работы с Kafka на Golang (sarama pkg)
Исходная цель: получить "репитер", т.е. все, что попадает в producer в кластере Kafka, в том же порядке , в полном составе, без задержек доставлялось до любого кол-ва consumer.

### 1. Один partition = один consumer Group
+ Желаемый эффект:<br> все сообщения, отправленные в producer доставляются до всех consumer group процессов в **том же порядке**, как были отправлены. Процессы получают сообщения без задержек, независимо от других процессов.
+ В реальности: <br>
  1. сообщения получает только один consumer. Второй просто ждет ребалансировки (или что-то другое?).
  2. ребалансировка затормаживает всех подключенных консьюмеров. 

### 2. Два partition каждый консьюмер получает половину
+ Желаемый эффект:<br> все сообщения, отправленные в producer доставляются до всех consumer group процессов в **том же порядке**, как были отправлены. Процессы получают сообщения без задержек, независимо от других процессов.
+ В реальности: <br>
  1. consumer получают сообщения случайно хотя и без повторов
  2. реализуется паттерн worker pool

### 3. Явное создание topic
+ Желаемый эффект:<br> автоматическое создание topic , с replicaSet обеспецивающим отказоустойчивость
+ В реальности: <br> без явного указания создания topic создается topic c 1 partition и 1 replicaSet. Причем если topic не было, то topic создает не только producer, но и consumer, что возможно специфика реализации пакет sarama под Golang

### 4. Отказоустойчивость в кластере
+ Желаемый эффект:<br> при replicat set равном кол-ву нодов кластера отключение любого из нодов не влияет на доставку сообщений. Partition = 1 т.к. требуется гарантия последовательности сообщений. 
+ В реальности: <br> возможно особенности реализации пакета sarama, но consumer так и не подхватил сообщения после потери одного из двух нодов. Возможно еще дело в raft , которому для работы надо как минимум 3 кандидата, у мен же 2.

## Пример запуска для проблемы 2
producer
partitions = 2
```
% go run producer/producer.go -id=16 -p=2 -r=2
2024/11/16 10:56:03 Created topic singlefile_16 partitions 2 repicas 2
Message [1] sent partition(1)/offset(0)
Message [2] sent partition(0)/offset(0)
Message [3] sent partition(1)/offset(1)
Message [4] sent partition(0)/offset(1)
Message [5] sent partition(0)/offset(2)
Message [6] sent partition(1)/offset(2)
Message [7] sent partition(0)/offset(3)
Message [8] sent partition(0)/offset(4)
```
consumer 1 см контрольные значения "value"
```
% go run consumer/consumer.go -id=16 -new=false
2024/11/16 10:56:07 Consumer started for topic singlefile_16
2024/11/16 10:56:07 Message: partition 0 offset = 0 value = 1
2024/11/16 10:56:07 Message: partition 0 offset = 1 value = 3
2024/11/16 10:56:08 Message: partition 0 offset = 2 value = 4
2024/11/16 10:56:10 Message: partition 0 offset = 3 value = 6
2024/11/16 10:56:11 Message: partition 0 offset = 4 value = 7
```
consumer 2
```
% go run consumer/consumer.go -id=16 -new=false
2024/11/16 10:56:07 Consumer started for topic singlefile_16
2024/11/16 10:56:07 Message: partition 1 offset = 0 value = 0
2024/11/16 10:56:07 Message: partition 1 offset = 1 value = 2
2024/11/16 10:56:09 Message: partition 1 offset = 2 value = 5
```

  