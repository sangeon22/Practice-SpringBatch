# SpringBatch
Spring-Batch 연습



## 실습순서
- 스프링 배치 아키텍처 (Job, JobInstance, JobExecution, Step, StepExecution, 데이터 공유 ExecutionContext)
- Task 기반 배치와 Chunk 기반 배치
- JobParameters
- @JobScope와 @StepScope
- ItemReader Interface (CSV 파일 & JDBC 데이터 & JPA 데이터 읽기)
- ItemWriter Interface (CSV 파일 & JDBC 데이터 & JPA 데이터 쓰기)
- ItemProcessor Interface
- CSV 파일 데이터 읽고 MySQL에 INSERT
- 테스트 코드 작성
- JobExecutionListener, StepExecutionListener
- StepListener
- Skip Handling Exception
- Retry Handling Exception

- 회원등급 프로젝트
- 주문금액 집계 프로젝트
- 성능 개선과 성능 비교 (Async Step 적용, Partition Step 적용, Parallel Step 적용)
- 스프링 배치 설정과 실행 (jenkins scheduler)
