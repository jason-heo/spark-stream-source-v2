Spark Data Source API V2로 작성된 Spark Streaming Source 예제입니다.

Spark 2.4의 [RateStreamProvider.scala](https://github.com/apache/spark/blob/branch-2.4/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/RateStreamProvider.scala)를 분석하고 작성하였습니다.

작동 방식에 대해서는 소스 코드를 열심히 분석하고 직접 돌려서 로그 메시지를 확인하는 것이 제일 좋습니다.

간단한 작동 방식에 대해서는 http://jason-heo.github.io/bigdata/2020/08/15/spark-stream-source-v2.html 에 설명해두었습니다.
