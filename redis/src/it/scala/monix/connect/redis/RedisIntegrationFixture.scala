package monix.connect.redis

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import org.scalacheck.Gen

trait RedisIntegrationFixture {
  val redisUrl = "redis://localhost:6379"
  type K = String
  type V = Int
  val genRedisKey: Gen[K] = Gen.alphaStr
  val genRedisValue: Gen[V] = Gen.choose(0, 10000)
  val genRedisValues: Gen[List[V]] = for {
    n      <- Gen.chooseNum(2, 10)
    values <- Gen.listOfN(n, Gen.choose(0, 10000))
  } yield values

  implicit val connection: StatefulRedisConnection[String, String] = RedisClient.create(redisUrl).connect()

}
