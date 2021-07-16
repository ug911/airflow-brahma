import cryptocode

encoded = cryptocode.encrypt("mystring","mypassword")
## And then to decode it:
decoded = cryptocode.decrypt(encoded, "mypassword")