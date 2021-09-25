// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.example.account;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.UUID;

@JsonSerialize
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public record BankAccount(UUID accountNumber, String accountOwner, String securityCode, double balance) {
}


