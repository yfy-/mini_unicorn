package com.yfy.mini_unicorn.types

import com.yfy.mini_unicorn.Parameterizable

sealed trait EdgeType extends Serializable with Parameterizable{
  def name: String
  def isDirected: Boolean
  def src: VertexType
  def dest: VertexType
}

case object FriendEdge extends EdgeType {
  val name = "friend"
  val isDirected = false
  val src, dest = PersonVertex
}

