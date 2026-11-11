// Package judgestore persists judges and their assignments to workspaces:
// creating judge rows, resolving judges with their validation rules, and
// reading and adding workspace judge assignments.
//
// It is the persistence side of judges. Judge selection during checks lives
// in jobs/checker/judges, which never imports persistence; app/bootstrap and
// the settings code load assignments from here into that cache.
package judgestore
