(ns caribou.auth)

(import org.mindrot.jbcrypt.BCrypt)

(defn hash-pw
  "hash a password to store it in a password field"
  [pass]
  (. BCrypt hashpw pass (. BCrypt gensalt 12)))

(defn check-pw
  "check a raw password against a hash from the password field"
  [pass hash]
  (. BCrypt checkpw pass hash))
