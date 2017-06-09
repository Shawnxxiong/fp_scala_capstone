package observatory

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractionTest extends FunSuite {
  
  test("toCelcius") {
    assert(Extraction.toCelcius(32) === 0.0)
    assert(BigDecimal(Extraction.toCelcius(100)).setScale(2, BigDecimal.RoundingMode.HALF_UP) === 37.78)
  }
  
  test("extractYearFromFilename") {
    assert(Extraction.extractYearFromFileName("/1975.csv").getOrElse(0) === 1975)
    assert(Extraction.extractYearFromFileName("/C/User/shawn.xiong/1988.csv").getOrElse(0) === 1988)
    assert(Extraction.extractYearFromFileName("/200123.csv").getOrElse(0) === 0)
    assert(Extraction.extractYearFromFileName("/12.csv").getOrElse(0) === 0)
  }
  
}