sealed trait VertexType extends Serializable { def hitDataName: String }

case object PersonVertex extends VertexType { val hitDataName = "person specifier" }
case object PhotoVertex extends VertexType { val hitDataName = "photo specifier" }
