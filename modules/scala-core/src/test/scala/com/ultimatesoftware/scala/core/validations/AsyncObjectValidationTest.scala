// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.core.validations

import java.util.UUID

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ ExecutionContext, Future, TimeoutException }

class AsyncObjectValidationTest extends AsyncWordSpec with Matchers {
  import FieldValidators._
  import ValidationDSL._
  private implicit val ec: ExecutionContext = ExecutionContext.global

  // Sample Domain Model

  // Mock Service for Verifying a Tax Authority ID
  val MockTaxAuthorityVerifyService: UUID ⇒ Future[Boolean] = {
    case uuid if uuid.eq(Employee.TAX_AUTHORITY_ID) ⇒ Future { true }
    case uuid if uuid.eq(Employee.TAX_AUTHORITY_ID_TIMEOUT) ⇒ Future.failed(new TimeoutException(Employee.TAX_AUTHORITY_TIME_MESSAGE))
    case _ ⇒ Future { false }
  }

  case class TaxData(state: String, exemptions: Int, taxRate: Double, taxAuthorityId: UUID)
  case class Employee(name: String, salary: Double, codes: Seq[String], taxData: TaxData)
  object Employee {

    val NAME_ERR_MSG = "Name can not be blank"
    val SALARY_ERR_MSG = "Salary must be positive"
    val CODE_ERR_MSG = "Employee must have at least one code"
    val DUP_CODE_ERR_MSG = "No duplicate Codes allowed"
    val TAX_STATE_ERR_MSG = "Tax State can not be blank"
    val TAX_EXEMPTION_ERR_MSG = "Exemption must be zero or greater"
    val TAX_RATE_ERR_MSG = "Tax rate is out of range"
    val TAX_AUTHORITY_ID_ERR_MSG = "The Tax Authority ID could not be validated"
    val TAX_AUTHORITY_TIME_MESSAGE = "The Tax Authority request timed out"

    val TAX_AUTHORITY_ID: UUID = UUID.randomUUID()
    val TAX_AUTHORITY_ID_TIMEOUT: UUID = UUID.randomUUID()

    // Async Validator that uses a mock service to make a latent I/O call
    val ValidTaxAuthorityId: AsyncValidator[UUID] =
      AsyncValidateWith(id ⇒ MockTaxAuthorityVerifyService(id), Some(TAX_AUTHORITY_ID_ERR_MSG))

    val EmployeeAsyncValidator: AsyncObjectValidator[Employee] = AsyncObjectValidator[Employee]({ employee ⇒
      Seq(
        employee.name mustBe NonEmptyString orElse NAME_ERR_MSG,
        employee.salary mustBe PositiveNumber[Double] orElse SALARY_ERR_MSG,
        employee.codes must ContainAtLeastOneItem[String] orElse CODE_ERR_MSG,
        employee.codes must ContainOnlyOneOfEach[String, String](identity) orElse DUP_CODE_ERR_MSG,
        employee.taxData.state mustBe NonEmptyString orElse TAX_STATE_ERR_MSG,
        employee.taxData.exemptions mustBe NonNegativeNumber[Int] orElse TAX_EXEMPTION_ERR_MSG,
        employee.taxData.taxRate mustBe InTheRange[Double](0, .9) orElse TAX_RATE_ERR_MSG,
        employee.taxData.taxAuthorityId mustBe ValidTaxAuthorityId orElse TAX_AUTHORITY_ID_ERR_MSG)
    })
  }
  // End Sample Domain Model

  import Employee._

  "AsyncValidator" should {

    "validate a good Tax Authority ID" in {
      val futureRes = TAX_AUTHORITY_ID mustBe ValidTaxAuthorityId orElse DEFAULT_VALIDATION_MESSAGE
      futureRes map (_ shouldBe Right(TAX_AUTHORITY_ID))
    }
  }

  "AsyncObjectValidator" should {

    "validate an Employee" in {
      val employee = Employee("Ulti Tim", 100000.00, List("CODE"), TaxData("PA", 1, .20, TAX_AUTHORITY_ID))
      val futureRes = employee mustSatisfy EmployeeAsyncValidator
      futureRes map (_ shouldBe Right(employee))
    }

    "invalidate an Employee with a blank name" in {
      val futureRes = Employee("", 100000.00, List("CODE"), TaxData("PA", 1, .20, TAX_AUTHORITY_ID)) mustSatisfy EmployeeAsyncValidator
      futureRes map (_ shouldBe Left(Seq(ValidationError(NAME_ERR_MSG))))
    }

    "invalidate an Employee with a negative salary" in {
      val futureRes = Employee("Ulti Tim", -100000.00, List("CODE"), TaxData("PA", 1, .20, TAX_AUTHORITY_ID)) mustSatisfy EmployeeAsyncValidator
      futureRes map (_ shouldBe Left(Seq(ValidationError(SALARY_ERR_MSG))))
    }

    "invalidate an Employee with an empty code list" in {
      val futureRes = Employee("Ulti Tim", 100000.00, List.empty, TaxData("PA", 1, .20, TAX_AUTHORITY_ID)) mustSatisfy EmployeeAsyncValidator
      futureRes map (_ shouldBe Left(Seq(ValidationError(CODE_ERR_MSG))))
    }

    "invalidate an Employee with with duplicate codes" in {
      val futureRes = Employee("Ulti Tim", 100000.00, List("CODE", "CODE"), TaxData("PA", 1, .20, TAX_AUTHORITY_ID)) mustSatisfy EmployeeAsyncValidator
      futureRes map (_ shouldBe Left(Seq(ValidationError(DUP_CODE_ERR_MSG))))
    }

    "invalidate an Employee because an async validation has timed out" in {
      val futureRes = Employee("Ulti Tim", 100000.00, List("CODE"), TaxData("PA", 1, .20, TAX_AUTHORITY_ID_TIMEOUT)) mustSatisfy EmployeeAsyncValidator
      futureRes map (_ shouldBe Left(Seq(ValidationError(TAX_AUTHORITY_TIME_MESSAGE))))
    }

    "invalidate an Employee with many invalid field values" in {
      val futureRes = Employee("", -100000.00, List.empty, TaxData("", -1, .20, UUID.randomUUID())) mustSatisfy EmployeeAsyncValidator
      futureRes map {
        _ shouldBe Left(Seq(
          ValidationError(NAME_ERR_MSG),
          ValidationError(SALARY_ERR_MSG),
          ValidationError(CODE_ERR_MSG),
          ValidationError(TAX_STATE_ERR_MSG),
          ValidationError(TAX_EXEMPTION_ERR_MSG),
          ValidationError(TAX_AUTHORITY_ID_ERR_MSG)))
      }
    }
  }
}
