package planner

// MergeConstraints merges two PlannerConstraints (set union of column names).
// Multiple flipped joins contribute extra constraints to a parent join; these
// need to be merged. If either input is nil, the other is returned.
func MergeConstraints(a, b PlannerConstraint) PlannerConstraint {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	result := make(PlannerConstraint, len(a)+len(b))
	for k := range a {
		result[k] = struct{}{}
	}
	for k := range b {
		result[k] = struct{}{}
	}
	return result
}
