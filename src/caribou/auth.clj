(ns caribou.auth)

(import org.mindrot.jbcrypt.BCrypt)
(import [java.security MessageDigest])

(defn hash-bcrypt
  "hash a password to store it in a password field"
  [pass]
  (. BCrypt hashpw pass (. BCrypt gensalt 13)))

(defn check-bcrypt
  "check a raw password against a hash from the password field"
  [pass hash]
  (. BCrypt checkpw pass hash))

(def ^{:dynamic true} *default-hash* "SHA-256")
 
(defn hexdigest
  "Returns the hex digest of an object. Expects a string as input."
  ([input] (hexdigest input *default-hash*))
  ([input hash-algo]
     (if (string? input)
       (let [hash (MessageDigest/getInstance hash-algo)]
         (. hash update (.getBytes input))
         (let [digest (.digest hash)]
           (apply str (map #(format "%02x" (bit-and % 0xff)) digest))))
       (do
         (println "Invalid input! Expected string, got" (type input))
         nil))))

(defn checkhex [obj ref-hash]
  (= ref-hash (hexdigest obj)))

(def hash-password hexdigest)
(def check-password checkhex)
