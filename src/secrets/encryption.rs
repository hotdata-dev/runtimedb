//! Low-level encryption/decryption for secret values.
//!
//! Format: ['R','V','S','1'][scheme][key_version][12-byte nonce][ciphertext...]
//! - Magic: 4 bytes "RVS1"
//! - Scheme: 1 byte (0x01 = AES-256-GCM-SIV)
//! - Key version: 1 byte (0x01 = first key)
//! - Nonce: 12 bytes
//! - Ciphertext: remaining bytes

use aes_gcm_siv::{
    aead::{Aead, KeyInit, OsRng},
    Aes256GcmSiv, Nonce,
};

const MAGIC: &[u8; 4] = b"RVS1";
const SCHEME_AES_256_GCM_SIV: u8 = 0x01;
const KEY_VERSION_1: u8 = 0x01;
const NONCE_SIZE: usize = 12;
const HEADER_SIZE: usize = 4 + 1 + 1 + NONCE_SIZE; // 18 bytes

/// Encrypts plaintext using AES-256-GCM-SIV.
///
/// # Arguments
/// * `key` - 32-byte encryption key
/// * `plaintext` - Data to encrypt
/// * `aad` - Associated authenticated data (secret name, prevents blob swapping)
pub fn encrypt(key: &[u8; 32], plaintext: &[u8], aad: &str) -> Result<Vec<u8>, EncryptError> {
    let cipher =
        Aes256GcmSiv::new_from_slice(key).map_err(|e| EncryptError::CipherInit(e.to_string()))?;

    // Generate random nonce
    let mut nonce_bytes = [0u8; NONCE_SIZE];
    aes_gcm_siv::aead::rand_core::RngCore::fill_bytes(&mut OsRng, &mut nonce_bytes);

    let nonce = Nonce::from(nonce_bytes);

    let ciphertext = cipher
        .encrypt(
            &nonce,
            aes_gcm_siv::aead::Payload {
                msg: plaintext,
                aad: aad.as_bytes(),
            },
        )
        .map_err(|e| EncryptError::Encryption(e.to_string()))?;

    // Build output: magic + scheme + key_version + nonce + ciphertext
    let mut output = Vec::with_capacity(HEADER_SIZE + ciphertext.len());
    output.extend_from_slice(MAGIC);
    output.push(SCHEME_AES_256_GCM_SIV);
    output.push(KEY_VERSION_1);
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(&ciphertext);

    Ok(output)
}

/// Decrypts ciphertext encrypted with `encrypt()`.
///
/// # Arguments
/// * `key` - 32-byte encryption key (must match key version in blob)
/// * `encrypted` - Encrypted blob from `encrypt()`
/// * `aad` - Associated authenticated data (must match value used during encryption)
pub fn decrypt(key: &[u8; 32], encrypted: &[u8], aad: &str) -> Result<Vec<u8>, DecryptError> {
    if encrypted.len() < HEADER_SIZE {
        return Err(DecryptError::TooShort);
    }

    // Verify magic
    if &encrypted[0..4] != MAGIC {
        return Err(DecryptError::InvalidMagic);
    }

    // Check scheme
    let scheme = encrypted[4];
    if scheme != SCHEME_AES_256_GCM_SIV {
        return Err(DecryptError::UnsupportedScheme(scheme));
    }

    // Check key version (for future key rotation support)
    let key_version = encrypted[5];
    if key_version != KEY_VERSION_1 {
        return Err(DecryptError::UnsupportedKeyVersion(key_version));
    }

    // Extract nonce and ciphertext
    let nonce_bytes: [u8; NONCE_SIZE] = encrypted[6..6 + NONCE_SIZE]
        .try_into()
        .expect("slice length matches NONCE_SIZE");
    let ciphertext = &encrypted[HEADER_SIZE..];

    let cipher =
        Aes256GcmSiv::new_from_slice(key).map_err(|e| DecryptError::CipherInit(e.to_string()))?;

    let nonce = Nonce::from(nonce_bytes);

    let plaintext = cipher
        .decrypt(
            &nonce,
            aes_gcm_siv::aead::Payload {
                msg: ciphertext,
                aad: aad.as_bytes(),
            },
        )
        .map_err(|_| DecryptError::AuthenticationFailed)?;

    Ok(plaintext)
}

#[derive(Debug, thiserror::Error)]
pub enum EncryptError {
    #[error("Failed to initialize cipher: {0}")]
    CipherInit(String),
    #[error("Encryption failed: {0}")]
    Encryption(String),
}

#[derive(Debug, thiserror::Error)]
pub enum DecryptError {
    #[error("Encrypted data too short")]
    TooShort,
    #[error("Invalid magic bytes (not a RuntimeDB secret)")]
    InvalidMagic,
    #[error("Unsupported encryption scheme: {0}")]
    UnsupportedScheme(u8),
    #[error("Unsupported key version: {0}")]
    UnsupportedKeyVersion(u8),
    #[error("Failed to initialize cipher: {0}")]
    CipherInit(String),
    #[error("Authentication failed (wrong key, corrupted data, or AAD mismatch)")]
    AuthenticationFailed,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        [0x42; 32] // Deterministic key for tests
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = test_key();
        let plaintext = b"my-secret-password";
        let aad = "my-secret-name";

        let encrypted = encrypt(&key, plaintext, aad).unwrap();
        let decrypted = decrypt(&key, &encrypted, aad).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypted_format() {
        let key = test_key();
        let encrypted = encrypt(&key, b"test", "name").unwrap();

        assert!(encrypted.len() >= HEADER_SIZE);
        assert_eq!(&encrypted[0..4], MAGIC);
        assert_eq!(encrypted[4], SCHEME_AES_256_GCM_SIV);
        assert_eq!(encrypted[5], KEY_VERSION_1);
    }

    #[test]
    fn test_wrong_key_fails() {
        let key1 = [0x42; 32];
        let key2 = [0x43; 32];
        let plaintext = b"secret";
        let aad = "name";

        let encrypted = encrypt(&key1, plaintext, aad).unwrap();
        let result = decrypt(&key2, &encrypted, aad);

        assert!(matches!(result, Err(DecryptError::AuthenticationFailed)));
    }

    #[test]
    fn test_wrong_aad_fails() {
        let key = test_key();
        let plaintext = b"secret";

        let encrypted = encrypt(&key, plaintext, "correct-name").unwrap();
        let result = decrypt(&key, &encrypted, "wrong-name");

        assert!(matches!(result, Err(DecryptError::AuthenticationFailed)));
    }

    #[test]
    fn test_corrupted_ciphertext_fails() {
        let key = test_key();
        let mut encrypted = encrypt(&key, b"secret", "name").unwrap();

        // Corrupt the ciphertext
        if let Some(byte) = encrypted.last_mut() {
            *byte ^= 0xFF;
        }

        let result = decrypt(&key, &encrypted, "name");
        assert!(matches!(result, Err(DecryptError::AuthenticationFailed)));
    }

    #[test]
    fn test_invalid_magic_fails() {
        let key = test_key();
        let mut encrypted = encrypt(&key, b"secret", "name").unwrap();
        encrypted[0] = 0x00;

        let result = decrypt(&key, &encrypted, "name");
        assert!(matches!(result, Err(DecryptError::InvalidMagic)));
    }

    #[test]
    fn test_too_short_fails() {
        let key = test_key();
        let result = decrypt(&key, &[0; 10], "name");
        assert!(matches!(result, Err(DecryptError::TooShort)));
    }

    #[test]
    fn test_empty_plaintext() {
        let key = test_key();
        let encrypted = encrypt(&key, b"", "name").unwrap();
        let decrypted = decrypt(&key, &encrypted, "name").unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn test_large_plaintext() {
        let key = test_key();
        let plaintext = vec![0xAB; 10000];
        let encrypted = encrypt(&key, &plaintext, "name").unwrap();
        let decrypted = decrypt(&key, &encrypted, "name").unwrap();
        assert_eq!(decrypted, plaintext);
    }
}
