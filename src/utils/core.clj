(ns utils.core
  (:require [zeromq.zmq :as zmq]
            [clojure.tools.logging :as log])
  (:import (java.net ServerSocket)))

(defn get-free-port []
  (let [socket (ServerSocket. 0)]
    (.close socket)
    (.getLocalPort socket)))

(defn debug [x]
  (prn x)
  x)

(defn read-sock [socket]
  (log/info "Reading from socket: " socket)
  (read-string (zmq/receive-str socket)))

(defn write-sock [socket data]
  (log/info "Writing to socket: " socket " (Data: " data ")")
  (zmq/send-str socket (prn-str data)))