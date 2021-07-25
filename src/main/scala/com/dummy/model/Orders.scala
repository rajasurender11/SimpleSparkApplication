package com.dummy.model

case class Orders(transDt:String,
                  customerName:String,
                  productId:String,
                  productName:String,
                  modeOfTransaction:Int,
                  price:Int) extends Product with Serializable
