package com.example.utils

import org.junit.{Assert, Test}
import org.hamcrest.CoreMatchers._

class TestAppConfig {

  @Test
  def testApplicationName() = {
    Assert.assertThat(AppConfig.getApplicationName, is("example-kafka"))
  }
}
