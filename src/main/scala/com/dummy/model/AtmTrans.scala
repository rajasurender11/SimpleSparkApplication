package com.dummy.model

case class AtmTrans(accountId:String,
                    atmId:String,
                    transDt:String,
                    amount:Long,
                    status:String)extends Product with Serializable
