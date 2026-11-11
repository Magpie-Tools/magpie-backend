// Package rotatingproxy runs the rotating proxy listeners. A listener
// authenticates its client, selects the next upstream route from committed
// evidence and forwards HTTP, CONNECT or SOCKS traffic through it.
//
// Its imports are pinned by internal/archtest together with the checker and
// the proxy queue.
package rotatingproxy
