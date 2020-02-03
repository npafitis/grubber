(ns utils.core
  (:import (java.net ServerSocket)))

(defn get-free-port []
  (let [socket (ServerSocket. 0)]
    (.close socket)
    (.getLocalPort socket)))

(defn debug [x]
  (prn x)
  x)
