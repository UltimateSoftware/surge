// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.core.validations

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class ObjectValidationTest extends AsyncWordSpec with Matchers {
  import FieldValidators._
  import ValidationDSL._

  // Sample Domain Model
  case class TaxData(state: String, exemptions: Int, taxRate: Double, country: String = "US", nationalId: String)
  case class Employee(name: String, salary: Double, codes: Seq[String], taxData: TaxData)
  object Employee {

    val NAME_ERR_MSG = "Name can not be blank"
    val SALARY_ERR_MSG = "Salary must be positive"
    val CODE_ERR_MSG = "Employee must have at least one code"
    val DUP_CODE_ERR_MSG = "No duplicate Codes allowed"
    val TAX_DATE_ERR_MSG = "Tax data is invalid"
    val TAX_STATE_ERR_MSG = "Tax State can not be blank"
    val TAX_EXEMPTION_ERR_MSG = "Exemption must be zero or greater"
    val TAX_RATE_ERR_MSG = "Tax rate is out of range"
    val US_TAX_ID_ERR_MSG = "National ID must be a valid US SSN"
    val CA_TAX_ID_ERR_MSG = "National ID must be a valid Canadian SIN"
    val TAX_ID_ERR_MSG = "National ID must be blank"

    val ValidNationalTaxId: Validator[Employee] = { input ⇒
      val res = input match {
        case (employee, _) if employee.taxData.country.equalsIgnoreCase("US") ⇒
          employee.taxData.nationalId mustBe ValidUsSSN orElse US_TAX_ID_ERR_MSG
        case (employee, _) if employee.taxData.country.equalsIgnoreCase("CA") ⇒
          employee.taxData.nationalId mustBe ValidCanadianSIN orElse CA_TAX_ID_ERR_MSG
        case (employee, _) ⇒ employee.taxData.nationalId mustBe NonEmptyString orElse TAX_ID_ERR_MSG
      }
      res match {
        case Right(st) ⇒ Right(input._1)
        case Left(lvm) ⇒ Left(lvm)
      }
    }

    // TODO / WIP - Alternative Forms
    //
    //    def ValidNationalTaxIdIn: String ⇒ Validator[String] = { country ⇒
    //      country.toUpperCase() match {
    //        case "US" ⇒ ValidUsSSN
    //        case "CA" ⇒ ValidCanadianSIN
    //        case _    ⇒ NonEmptyString
    //      }
    //    }

    //    def ValidNationalTaxId2(taxData: TaxData): ValidationResult[TaxData] = {
    //      taxData mustBe SatisfyAtLeastOneOf[TaxData](
    //        (taxData.country mustBe EqualTo("US") and (taxData.nationalId mustBe ValidUsSSN) orElse US_TAX_ID_ERR_MSG,
    //          (taxData.country mustBe EqualTo("CA")) and (taxData.nationalId mustBe ValidCanadianSIN) orElse CA_TAX_ID_ERR_MSG)
    //      )
    //    }
    //

    val EmployeeValidator: ObjectValidator[Employee] = ObjectValidator[Employee]({ employee ⇒
      Seq(
        employee.name mustBe NonEmptyString orElse NAME_ERR_MSG,
        employee.salary mustBe PositiveNumber[Double] orElse SALARY_ERR_MSG,
        employee.codes must ContainAtLeastOneItem[String] orElse CODE_ERR_MSG,
        employee.codes must ContainOnlyOneOfEach[String, String](identity) orElse DUP_CODE_ERR_MSG,
        employee.taxData.state mustBe NonEmptyString orElse TAX_STATE_ERR_MSG,
        employee.taxData.exemptions.toDouble mustBe NonNegativeNumber[Double] orElse TAX_EXEMPTION_ERR_MSG,
        employee.taxData.taxRate mustBe InTheRange(0, .5) orElse TAX_RATE_ERR_MSG,
        employee mustHave ValidNationalTaxId orElseDefaultMessage)
    })
  }

  import Employee._

  "ObjectValidator" should {

    val validEmployee = Employee("Ulti Tim", 100000.00, List("CODE"), TaxData("PA", 1, .20, "US", "123-45-6789"))

    "validate an Employee" in {
      val result = validEmployee mustSatisfy EmployeeValidator
      result should be(Right(validEmployee))
    }

    "invalidate an Employee with a blank name" in {
      val result = validEmployee.copy(name = "") mustSatisfy EmployeeValidator
      result should be(Left(Seq(
        ValidationError(NAME_ERR_MSG))))
    }

    "invalidate an Employee with a negative salary" in {
      val result = validEmployee.copy(salary = -100000) mustSatisfy EmployeeValidator
      result should be(Left(Seq(
        ValidationError(SALARY_ERR_MSG))))
    }

    "invalidate an Employee with an empty code list" in {
      val result = validEmployee.copy(codes = List.empty) mustSatisfy EmployeeValidator
      result should be(Left(Seq(
        ValidationError(CODE_ERR_MSG))))
    }

    "invalidate an Employee with with duplicate codes" in {
      val result = validEmployee.copy(codes = List("CODE", "CODE")) mustSatisfy EmployeeValidator
      result should be(Left(Seq(
        ValidationError(DUP_CODE_ERR_MSG))))
    }

    "invalidate an Employee with a bad tax id" in {
      val result = validEmployee.copy(taxData = validEmployee.taxData.copy(nationalId = "12-3456789")) mustSatisfy EmployeeValidator
      result should be(Left(Seq(
        ValidationError(US_TAX_ID_ERR_MSG))))
    }

    "invalidate an Employee with many invalid values" in {
      val invalidEmployee = validEmployee.copy(
        name = "",
        salary = -100000.00,
        codes = List.empty,
        taxData = validEmployee.taxData.copy(state = "", exemptions = -1, nationalId = "1-45-6789"))

      val result = invalidEmployee mustSatisfy EmployeeValidator
      result should be(Left(Seq(
        ValidationError(NAME_ERR_MSG),
        ValidationError(SALARY_ERR_MSG),
        ValidationError(CODE_ERR_MSG),
        ValidationError(TAX_STATE_ERR_MSG),
        ValidationError(TAX_EXEMPTION_ERR_MSG),
        ValidationError(US_TAX_ID_ERR_MSG))))
    }

    "be able to be used as an AsyncObjectValidator implicitly" in {
      val asyncValidator: AsyncObjectValidator[Employee] = EmployeeValidator
      val result = validEmployee mustSatisfy asyncValidator
      result.map(_ should be(Right(validEmployee)))
    }
  }
}
