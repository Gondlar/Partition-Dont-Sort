package de.unikl.cs.dbis.waves

import org.apache.spark.sql.connector.write.{
    WriteBuilder,
    SupportsTruncate,
    Write,
    BatchWrite,
    DataWriterFactory,
    PhysicalWriteInfo,
    WriterCommitMessage,
    LogicalWriteInfo,
    SupportsOverwrite
}
import org.apache.spark.sql.sources.Filter

case class WavesWriteBuilder (
    table : WavesTable,
    logicalInfo : LogicalWriteInfo
) extends WriteBuilder
    with SupportsTruncate
    with Write
    with BatchWrite {

    var doTruncate = false

    override def truncate(): WriteBuilder = {
        doTruncate = true
        this
    }

    override def build(): Write = this

    override def description(): String
        = s"waves Write ${if (doTruncate) "truncated" else ""}"
    
    override def toBatch(): BatchWrite = this

    private var delegate : Option[BatchWrite] = None
    private var writeFolder : Option[PartitionFolder] = None


    override def createBatchWriterFactory(physicalInfo: PhysicalWriteInfo): DataWriterFactory = {
        assert(this.delegate.isEmpty)
        assert(this.writeFolder.isEmpty)

        val folder = if (doTruncate) table.truncate() else table.insertLocation()
        this.writeFolder = Some(folder)
        val delegate = table.makeDelegateTable(folder)
                            .newWriteBuilder(logicalInfo)
                            .build()
                            .toBatch()
        this.delegate = Some(delegate)
        delegate.createBatchWriterFactory(physicalInfo)
    }

    // Mimic Parquet behaviour
    override def useCommitCoordinator(): Boolean = false

    override def onDataWriterCommit(message: WriterCommitMessage): Unit =  {
        assert(this.delegate.isDefined)
        assert(this.writeFolder.isDefined)

        delegate.get.onDataWriterCommit(message)
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
        assert(this.delegate.isDefined)
        assert(this.writeFolder.isDefined)

        delegate.get.commit(messages)
        table.writePartitionScheme()
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {
        assert(this.delegate.isDefined)
        assert(this.writeFolder.isDefined)

        try {
            delegate.get.abort(messages)
        } finally this.writeFolder.get.delete(table.fs)
    }
  
}
