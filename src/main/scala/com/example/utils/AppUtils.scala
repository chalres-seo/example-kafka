package com.example.utils

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.{ExecutorService, Executors}

import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

object AppUtils extends LazyLogging {

  def writeFile(savePathString: String, records: Vector[String], append: Boolean) = {
    logger.info(s"write to file. path: $savePathString, append: $append")

    val savePath = Paths.get(savePathString)

    if(Files.notExists(savePath)) {
      logger.info("file path is no exist. create path.")
      Files.createDirectories(savePath.getParent)
    }

    val fileWriter = if (append) {
      Files.newBufferedWriter(savePath,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND)
    } else {
      Files.newBufferedWriter(savePath,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE)
    }

    records.foreach { record =>
      Try {
        fileWriter.write(record)
        fileWriter.newLine()
      } match {
        case Success(_) =>
          logger.debug(s"write record: $record")
        case Failure(e) =>
          logger.error(e.getMessage)
          logger.error(record)
      }
    }

    logger.info("file flushing and close.")
    fileWriter.flush()
    fileWriter.close()
  }
}