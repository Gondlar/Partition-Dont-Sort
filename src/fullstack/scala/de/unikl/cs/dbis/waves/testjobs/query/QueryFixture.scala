package de.unikl.cs.dbis.waves.testjobs.query

import de.unikl.cs.dbis.waves.util.Logger
import de.unikl.cs.dbis.waves.testjobs.IntegrationFixture
import de.unikl.cs.dbis.waves.testjobs.split.Plain

trait QueryFixture extends IntegrationFixture {
  override protected def beforeEach() = {
    super.beforeEach()

    Plain.main(args)
    clearLogs()
  }

  def queryWithResult(
    result: String,
    mode: Boolean,
    action: => Unit,
    additionalLogs: Seq[String] = Seq.empty
  ) = {
    "read the data correcytly" in {
      When("we run the job")
      action

      Then("the log contains what happened")
      val (events, data) = assertLogProperties()
      val modeLogs = if (mode) Seq("'build-scan'", "'chose-buckets'", "'scan-built'") ++ additionalLogs else Seq.empty
      events should contain theSameElementsInOrderAs ((Seq("'query-start'","'query-run'") ++ modeLogs) :+ "'query-end'")
      data(events.indexOf("'query-run'")) should equal (s"'$mode'")
      data(events.indexOf("'query-end'")) should equal (s"'$result'")
    }
  }
}
