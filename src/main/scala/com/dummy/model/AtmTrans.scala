package com.dummy.model

case class AtmTrans(accountId:String,
                    atmId:String,
                    transDt:String,
                    amount:String,
                    status:String)extends Product with Serializable
