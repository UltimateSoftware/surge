package com.example.exception

import java.util.UUID

class InsufficientFundsException(accountNumber: UUID) extends RuntimeException(s"Insufficient Funds in account $accountNumber to complete this transaction")
class AccountDoesNotExistException(accountNumber: UUID) extends RuntimeException(s"Account with id $accountNumber does not exist")