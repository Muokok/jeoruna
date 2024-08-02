**이러나 프로젝트 개발자분의 허락 하에 fork 하였습니다.**


**공부 차원에서 이러나 프로젝트를 분석하여 기록하고자 합니다.**

|<img src = "https://github.com/Eruna-Jeoruna/jeoruna/assets/47443884/86d73e0e-fdc1-4009-b809-f88ce94ad1e2" width = "70%" height = "70%">|
|:--:| 
| *[prography.org](https://prography.org)* |
# 이러나(Eruna)

그룹 구성원이 모두 일어나야 종료되는 그룹 알람 서비스 입니다.

- 소개 포스터 : [post.md](https://github.com/Eruna-Jeoruna/jeoruna/blob/main/post.md)
- 커밋 컨벤션 : [convention.md](https://github.com/Eruna-Jeoruna/jeoruna/blob/main/convention.md)



# 목차
- [시작하기](#시작하기)
  - [application.yml](#application.yml)
  - [Build](#Build)
  - [Jar 실행](#Jar-실행)
  - [배포](#배포)
- [구현](#구현)
- [테스트](#테스트)
- [인프라 스트럭처](#인프라-스트럭처)

# 시작하기

## application.yml

### JDBC

본 프로젝트는 MySQL Jdbc를 사용합니다. 

EC2 RDS 상에서 MariaDB를 사용하고 있어 거의 전부 호환되는 MySQL를 사용합니다.

MySQL의 라이센스 문제와 MariaDB의 쿼리 성능 및 본 프로젝트에서 아직 사용되지는 않았지만 스레드 풀링 같은 성 등의 이유로 마이그레이션 예정입니다.

[MySQL vs MariaDB](https://aws.amazon.com/ko/compare/the-difference-between-mariadb-vs-mysql/)

뿐만 아니라, 본 프로젝트 entity 도메인에서 group 예약어 이슈로 인해 groups라는 명칭으로 entity 테이블을 사용했지만, MySQL 8.0.2 버전에서 groups라는 예약어가 새로 생성되어 마이그레이션이 필요합니다.

```yaml
spring:
  datasource:
    driver-class-name: com.mysql.cj.jdbc.Driver
    url: jdbc:mysql://[RDS-url]:3306/eruna?serverTimezone=Asia
    username: 
    password:
```

- [RDS-url]을 사용하는 db의 url로 변경합니다.

### JPA

```yaml
spring:
  jpa:
    hibernate:
      ddl-auto: create
```

- 첫 실행 시 프로젝트에서 사용되는 DB 테이블들을 생성합니다.
- DB 데이터가 존재하는 경우 삭제될 수 있기 때문에 첫 실행 이후에는 주석 처리 혹은 update로 설정합니다.

### Redis

```yaml
spring:
  data:
    redis:
      host:[Redis-url]
      port: 6379
```

- 본 프로젝트에서는 기상 정보를 캐싱하기 위해 Redis를 사용합니다.
- AWS에서 Elasticache를 통해 구축했기 때문에 host 란에 Elasticache 엔드포인트를 기입합니다.

### Batch

```yaml
spring:	
  batch:
    jdbc:
      initialize-schema: always # spring batch 사용 table 생성
```

- 본 프로젝트에서는 당일 요일에 해당하는 알람들을 가져오기 위해 batch를 사용합니다.
- 첫 실행시 batch에서 필요한 테이블을 생성하기 위해 initialize-schema 를 always로 설정합니다.
- 이후 batch 테이블 초기화를 막으려면 never로 설정할 수 있습니다.

### 전체 application.yml

```yaml
spring:
  datasource:
    driver-class-name: com.mysql.cj.jdbc.Driver
    url: jdbc:mysql://[RDS-url]:3306/eruna?serverTimezone=Asia/Seoul&characterEncoding=UTF-8
    username: 
    password: 
  batch:
    jdbc:
      initialize-schema: always
  jpa:
    show-sql: true
      hibernate:
        ddl-auto: create
  data:
    redis:
      host: [redis-url]
      port: 6379

```

## Build

### Intellij 사용하기

![image](https://github.com/Eruna-Jeoruna/jeoruna/assets/47443884/5bd14945-71fc-490b-b700-c73c9deef791)

- 프로젝트를 inteillj로 열면 다음 화면이 나옵니다.

![image](https://github.com/Eruna-Jeoruna/jeoruna/assets/47443884/1c1bd552-bc5d-41cf-aa0e-f25e56a568a8)

- 우측 상단에 gradle를 클릭합니다.
- gradle 아이콘이 보이지 않는다면 view → tool windows → gradle로 찾을 수 있습니다.

![image](https://github.com/Eruna-Jeoruna/jeoruna/assets/47443884/1227ab07-8f0d-4dc7-b2ea-62aba5e076d7)

- 열린 gradle 윈도우에서 bootJar를 더블 클릭 하여 실행할 수 있습니다.
- 이후 상단처럼 프로젝트 실행 란에 bootJar가 들어간 것을 확인할 수 있고, 초록생 삼각형 아이콘을 클릭하여 다시 빌드할 수 있습니다.

### Cmd 사용하기

```yaml
C:\...\jeoruna
```

- 본 프로젝트를 클론 한 후 jeoruna 경로에서 cmd창을 생성합니다.

```yaml
gradlew build
```

- 다음 명령어로 빌드가 가능합니다.

```yaml
gradlew build -x test
```

- 만약 테스트를 skip하고 싶다면 다음 -x test옵션을 붙여줍니다.

![image](https://github.com/Eruna-Jeoruna/jeoruna/assets/47443884/38b1811a-780d-4e43-8846-3e4408f7a4b5)

## Jar 실행

![image](https://github.com/Eruna-Jeoruna/jeoruna/assets/47443884/f2691e7e-8c08-41c0-930e-c4bd00d5ea43)

- 위 빌드 과정을 거치면 프로젝트 build/libs 폴더에 다음 jar 파일이 생성됩니다.

```yaml
  java -jar eruna-0.0.1-SNAPSHOT.jar
```

- 다음 명령어를 통해 jar을 실행합니다.

```yaml
java -jar eruna-0.0.1-SNAPSHOT.jar —spring.profiles.active=prod

```

- 배포 환경에서 yml 프로파일을 다르게 설정해주고 싶을 때 `-spring.profiles.active=[profile name]` 옵션을 설정할 수 있습니다.

## 배포
다음 링크에서 aws ec2 환경에서 Nginx, Docker를 사용해 배포하는 과정을 소개합니다. - [링크](https://github.com/Eruna-Jeoruna/jeoruna/blob/main/deployment.md)

# 구현

### 무한 알람

![image](https://github.com/Eruna-Jeoruna/jeoruna/assets/47443884/90bde95c-f810-42f2-8e1e-fe82361cf582)

1. 매일 정오 저장된 알람 스케줄러 작성
2. 알람 이벤트 발생
3. FCM 무한 알람 생성

자세한 알람 플로우와 코드는 다음 링크를 참조해주세요 : [링크](https://www.notion.so/wakeup-SSE-Redis-023eabe116f14e71af2a8c8c010cdadf?pvs=21)

# 테스트

### BatchJobLaunchTests

DB에 저장된 알람들을 스프링 배치로 읽어 스케줄러에 등록하는 로직과 스케줄러에 등록된 알람이 울린 후 기상 요청이 들어왔을 때 실행되는 로직을 테스트합니다. 

자세한 내용은 다음 링크를 참고해주세요 : [링크](https://skitter-cathedral-fc6.notion.site/Spring-Batch-Quartz-92a860ad8bd64547a3e17baa2bc02432?pvs=4)


# 인프라 스트럭처

![Group 18 (1)](https://github.com/Eruna-Jeoruna/jeoruna/assets/47443884/07bd9b76-2b88-4dba-876a-8db103c84194)


