package monix.connect.redis

import io.lettuce.core.{KeyValue, ScoredValue}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RedisSortedSetSuite
  extends AnyFlatSpec with Matchers with RedisIntegrationFixture {

  "bzpopmin" should "return None when the timeout expires" in {
    val key: K = genRedisKey.sample.get

    val t: Task[KeyValue[K, ScoredValue[String]]] =
      RedisSortedSet.bzpopmin(1L, key)

    val r: KeyValue[K, ScoredValue[String]] = t.runSyncUnsafe()
    r shouldBe None
  }

  "bzpopmax" should "return None when the timeout expires" in {
    val key: K = genRedisKey.sample.get

    val t: Task[KeyValue[K, ScoredValue[String]]] =
      RedisSortedSet.bzpopmax(1L, key)

    val r: KeyValue[K, ScoredValue[String]] = t.runSyncUnsafe()
    r shouldBe None
  }

  "zrange" should "return an empty array when the key does not exist" in {
    val key: K = genRedisKey.sample.get

    val t: Task[List[String]] =
      RedisSortedSet.zrange(key, 0 , 1).toListL

    val r: List[String] = t.runSyncUnsafe()
    r shouldBe empty
  }

  "zrank" should "return None when the key does not exist" in {
    val key: K = genRedisKey.sample.get
    val member: K = genRedisValue.sample.get.toString

    val t: Task[Long] =
      RedisSortedSet.zrank(key, member)

    val r: Long = t.runSyncUnsafe()
    r shouldBe None
  }
  it should "return None when the member does not exist at the key" in {
    val key: K = genRedisKey.sample.get
    val member: K = genRedisValue.sample.get.toString
    val score: V = genRedisValue.sample.get

    val missingMember: K = genRedisValue.sample.get.toString

    RedisSortedSet.zadd(key, score, member).runSyncUnsafe()
    RedisSortedSet.zrank(key, member).runSyncUnsafe() shouldEqual 0

    val t: Task[Long] =
      RedisSortedSet.zrank(key, missingMember)

    val r: Long = t.runSyncUnsafe()
    r shouldBe None
  }

  "zscore" should "return None when the key does not exist" in {
    val key: K = genRedisKey.sample.get
    val member: K = genRedisValue.sample.get.toString

    val t: Task[Double] =
      RedisSortedSet.zscore(key, member)

    val r: Double = t.runSyncUnsafe()
    r shouldBe None
  }
  it should "return None when the member does not exist at the key" in {
    val key: K = genRedisKey.sample.get
    val member: K = genRedisValue.sample.get.toString
    val score: V = genRedisValue.sample.get

    val missingMember: K = genRedisValue.sample.get.toString

    RedisSortedSet.zadd(key, score, member).runSyncUnsafe()
    RedisSortedSet.zscore(key, member).runSyncUnsafe() shouldEqual score

    val t: Task[Double] =
      RedisSortedSet.zscore(key, missingMember)

    val r: Double = t.runSyncUnsafe()
    r shouldBe None
  }





}
