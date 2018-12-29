package com.aliyun.openservices.aliyun.log.producer.errors;

import com.aliyun.openservices.aliyun.log.producer.Attempt;
import com.aliyun.openservices.aliyun.log.producer.Result;
import java.util.List;

public class ResultFailedException extends ProducerException {

  private final Result result;

  public ResultFailedException(Result result) {
    this.result = result;
  }

  public Result getResult() {
    return result;
  }

  public String getErrorCode() {
    return result.getErrorCode();
  }

  public String getErrorMessage() {
    return result.getErrorMessage();
  }

  public List<Attempt> getReservedAttempts() {
    return result.getReservedAttempts();
  }
}
