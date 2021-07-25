package com.dummy.model

case class AccountsProfile(accountNo:String,
                           bankName:String,
                           custName:String,
                           gender:String,
                           phNo:String) extends Product with Serializable


