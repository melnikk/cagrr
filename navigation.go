package cagrr

// Is args to be current position of repar
func (n *Navigation) Is(cluster, keyspace, table string) bool {
	clusterNeed := n.Cluster == "" || (cluster == n.Cluster)
	keyspaceNeed := n.Keyspace == "" || (keyspace == n.Keyspace)
	tableNeed := n.Table == "" || (table == n.Table)
	return clusterNeed && keyspaceNeed && tableNeed
}
