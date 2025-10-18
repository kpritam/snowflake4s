package io.snowflake4s

/** Root error type for all snowflake4s errors. */
sealed abstract class SnowflakeError extends Product with Serializable {
  def message: String
}

object SnowflakeError {

  /** Errors related to decoding values from ResultSet. */
  sealed abstract class DecodingError extends SnowflakeError

  object DecodingError {
    final case class NullValue(columnIndex: Int, targetType: String) extends DecodingError {
      def message: String = s"NULL value at column $columnIndex cannot be decoded as $targetType"
    }

    final case class TypeMismatch(columnIndex: Int, sourceType: String, targetType: String, cause: String)
        extends DecodingError {
      def message: String = s"Cannot decode column $columnIndex from $sourceType to $targetType: $cause"
    }

    final case class ColumnNotFound(columnName: String, availableColumns: List[String]) extends DecodingError {
      def message: String = s"Column '$columnName' not found. Available columns: ${availableColumns.mkString(", ")}"
    }

    final case class InvalidColumnIndex(index: Int, maxIndex: Int) extends DecodingError {
      def message: String = s"Column index $index is out of bounds (max: $maxIndex)"
    }

    final case class Custom(msg: String) extends DecodingError {
      def message: String = msg
    }
  }

  /** Errors related to SQL query execution. */
  sealed abstract class QueryError extends SnowflakeError

  object QueryError {
    final case class SqlException(
        sqlState: Option[String],
        errorCode: Int,
        msg: String,
        queryId: Option[String] = None,
        cause: Option[Throwable] = None
    ) extends QueryError {
      def message: String = {
        val base = sqlState match {
          case Some(state) => s"SQL error [$state:$errorCode]: $msg"
          case None        => s"SQL error [$errorCode]: $msg"
        }
        mkMessage(base, queryId, cause)
      }
    }

    final case class PreparedStatementError(msg: String, cause: Option[Throwable] = None) extends QueryError {
      def message: String = cause match {
        case Some(ex) => s"Failed to prepare statement: $msg - Cause: ${ex.getClass.getSimpleName}: ${ex.getMessage}"
        case None     => s"Failed to prepare statement: $msg"
      }
    }

    final case class ParameterBindingError(paramIndex: Int, msg: String, cause: Option[Throwable] = None)
        extends QueryError {
      def message: String = cause match {
        case Some(ex) =>
          s"Failed to bind parameter at index $paramIndex: $msg - Cause: ${ex.getClass.getSimpleName}: ${ex.getMessage}"
        case None => s"Failed to bind parameter at index $paramIndex: $msg"
      }
    }

    final case class ResultSetError(msg: String, queryId: Option[String] = None, cause: Option[Throwable] = None)
        extends QueryError {
      def message: String = mkMessage(s"ResultSet error: $msg", queryId, cause)
    }

    final case class TransactionError(action: String, msg: String, cause: Option[Throwable] = None) extends QueryError {
      def message: String = cause match {
        case Some(ex) => s"Transaction $action failed: $msg - Cause: ${ex.getClass.getSimpleName}: ${ex.getMessage}"
        case None     => s"Transaction $action failed: $msg"
      }
    }

    private def mkMessage(prefix: String, queryId: Option[String], cause: Option[Throwable]) = {
      val withQueryId = queryId match {
        case Some(id) if id.nonEmpty => s"$prefix (queryId=$id)"
        case _                       => prefix
      }
      cause match {
        case Some(ex) => s"$withQueryId - Cause: ${ex.getClass.getSimpleName}: ${ex.getMessage}"
        case None     => withQueryId
      }
    }
  }

  /** Errors related to connection management. */
  sealed abstract class ConnectionError extends SnowflakeError

  object ConnectionError {
    final case class FailedToConnect(msg: String, cause: Throwable) extends ConnectionError {
      def message: String = s"Failed to connect to Snowflake: $msg"
    }

    final case class ConnectionClosed(msg: String) extends ConnectionError {
      def message: String = s"Connection is closed: $msg"
    }

    final case class PoolExhausted(msg: String) extends ConnectionError {
      def message: String = s"Connection pool exhausted: $msg"
    }

    final case class Unknown(cause: Throwable) extends ConnectionError {
      def message: String = s"Unknown error: ${cause.getMessage}"
    }
  }

  /** Errors related to configuration. */
  sealed abstract class ConfigError extends SnowflakeError

  object ConfigError {
    final case class MissingRequired(field: String) extends ConfigError {
      def message: String = s"Missing required configuration field: $field"
    }

    final case class InvalidValue(field: String, value: String, reason: String) extends ConfigError {
      def message: String = s"Invalid value for $field='$value': $reason"
    }

    final case class LoadFailed(msg: String) extends ConfigError {
      def message: String = s"Failed to load configuration: $msg"
    }
  }
}
