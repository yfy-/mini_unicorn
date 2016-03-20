sealed trait EdgeType {
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

