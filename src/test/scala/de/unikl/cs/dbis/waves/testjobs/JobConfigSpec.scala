package de.unikl.cs.dbis.waves.testjobs

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SparkFixture

class JobConfigSpec extends WavesSpec {

  def valueWithDefault[T](fn: Symbol, value: T, default: T, env: Seq[(String, String)] = Seq.empty) = {
    s"fetch the ${fn.name}" when {
      "it is set" in {
        val config = new JobConfig(Map(((fn.name, value.toString) +: env):_*))
        config should have ( fn (value) )
      }
      "it is not set" in {
        val config = new JobConfig(Map(env:_*))
        config should have ( fn (default) )
      }
    }
  }

  "A JobConfig" can {
    "fetch string options" when {
      "they exist" in {
        val config = new JobConfig(Map(("Foo", "123")))
        config.getString("foO") should equal (Some("123"))
      }
      "they do not exist" in {
        val config = new JobConfig()
        config.getString("foO") should equal (None)
      }
    }
    "fetch Int options" when {
      "they exist" in {
        val config = new JobConfig(Map(("Foo", "123")))
        config.getInt("foO") should equal (Some(123L))
      }
      "they do not exist" in {
        val config = new JobConfig()
        config.getInt("foO") should equal (None)
      }
      "they are invalid" in {
        val config = new JobConfig(Map(("Foo", "abc")))
        config.getInt("foO") should equal (None)
      }
    }
    "fetch Long options" when {
      "they exist" in {
        val config = new JobConfig(Map(("Foo", "123")))
        config.getLong("foO") should equal (Some(123L))
      }
      "they do not exist" in {
        val config = new JobConfig()
        config.getLong("foO") should equal (None)
      }
      "they are invalid" in {
        val config = new JobConfig(Map(("Foo", "abc")))
        config.getLong("foO") should equal (None)
      }
    }
    "fetch Boolean options" when {
      "they exist" in {
        val config = new JobConfig(Map(("Foo", "true")))
        config.getBool("foO") should equal (Some(true))
      }
      "they do not exist" in {
        val config = new JobConfig()
        config.getBool("foO") should equal (None)
      }
      "they are invalid" in {
        val config = new JobConfig(Map(("Foo", "abc")))
        config.getBool("foO") should equal (None)
      }
    }
    "fetch the configured master" when {
      "it is set" in {
        val config = new JobConfig(Map(("master", "local")))
        config.master should equal (Some("local"))
        config shouldBe 'local
      }
      "it is not set" in {
        val config = new JobConfig()
        config.master should equal (None)
        config should not be 'local
      }
    }
    "fetch the filesystem" when {
      "it is set and the master is local" in {
        val config = new JobConfig(Map(("filesystem", "file:///foo"),("master", "local")))
        config.filesystem should equal ("file:///foo")
      }
      "it is set and the master is remote" in {
        val config = new JobConfig(Map(("filesystem", "file:///foo")))
        config.filesystem should equal ("file:///foo")
      }
      "it is not set and the master is local" in {
        val config = new JobConfig(Map(("master", "local")))
        config.filesystem should startWith ("file:///")
      }
      "it is not set and the master is remote" in {
        val config = new JobConfig()
        config.filesystem should startWith ("hdfs://namenode:9000")
      }
    }
    behave like valueWithDefault('inputPath, "/test", "file:///cluster-share/benchmarks/json/twitter/109g_multiple")
    behave like valueWithDefault('fallbackBlocksize, 123L, 128*1024*1024L)
    behave like valueWithDefault('sampleSize, 123L, 10*1024*1024L)
    behave like valueWithDefault('wavesPath, "asdf", "hdfs://namenode:9000/out/")
    behave like valueWithDefault('useWaves, false, true)
    behave like valueWithDefault('modifySchema, true, false)
    behave like valueWithDefault('completeScanColumn, "asdf", "user.name")
    behave like valueWithDefault('partialScanColumn, "asdf", "quoted_status.user.name")
    behave like valueWithDefault('scanValue, "asdf", "xx")
    "create a spark session from the configuration" in {
      val config = new JobConfig(Map(("master", "local")))
      val spark = config.makeSparkSession("Testsession")
      spark.sparkContext.appName should (equal ("Testsession") or equal (SparkFixture.TEST_SESSION_NAME))
      spark.sparkContext.master should equal ("local")
    }
    "be created from arguments" in {
      val config = JobConfig.fromArgs(Array("foo=bar", "asdf=1234"))
      config.getString("foo") should equal (Some("bar"))
      config.getLong("asdf") should equal (Some(1234L))
    }
    "detect malformed arguments" in {
      a [IllegalArgumentException] shouldBe thrownBy (JobConfig.fromArgs(Array("asdf")))
    }
    "detect repeated arguments" in {
      a [IllegalArgumentException] shouldBe thrownBy (JobConfig.fromArgs(Array("asdf=1234", "asdf=345")))
    }
  }
}