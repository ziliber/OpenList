package protondrive

/*
Package protondrive
Author: Da3zKi7<da3zki7@duck.com>
Date: 2025-09-18

Thanks to @henrybear327 for modded go-proton-api & Proton-API-Bridge

The power of open-source, the force of teamwork and the magic of reverse engineering!


D@' 3z K!7 - The King Of Cracking

Да здравствует Родина))
*/

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/ProtonMail/gopenpgp/v2/crypto"
	"github.com/henrybear327/go-proton-api"
)

func (d *ProtonDrive) uploadFile(ctx context.Context, parentLinkID string, file model.FileStreamer, up driver.UpdateProgress) (model.Obj, error) {
	_, err := d.getLink(ctx, parentLinkID)
	if err != nil {
		return nil, fmt.Errorf("failed to get parent link: %w", err)
	}

	var reader io.Reader
	// Use buffered reader with larger buffer for better performance
	var bufferSize int

	// File > 100MB (default)
	if file.GetSize() > d.ChunkSize*1024*1024 {
		// 256KB for large files
		bufferSize = 256 * 1024
		// File > 10MB
	} else if file.GetSize() > 10*1024*1024 {
		// 128KB for medium files
		bufferSize = 128 * 1024
	} else {
		// 64KB for small files
		bufferSize = 64 * 1024
	}

	// reader = bufio.NewReader(file)
	reader = bufio.NewReaderSize(file, bufferSize)
	reader = &driver.ReaderUpdatingProgress{
		Reader: &stream.SimpleReaderWithSize{
			Reader: reader,
			Size:   file.GetSize(),
		},
		UpdateProgress: up,
	}
	reader = driver.NewLimitedUploadStream(ctx, reader)

	id, _, err := d.protonDrive.UploadFileByReader(ctx, parentLinkID, file.GetName(), file.ModTime(), reader, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to upload file: %w", err)
	}

	return &model.Object{
		ID:       id,
		Name:     file.GetName(),
		Size:     file.GetSize(),
		Modified: file.ModTime(),
		IsFolder: false,
	}, nil
}

func (d *ProtonDrive) encryptFileName(ctx context.Context, name string, parentLinkID string) (string, error) {
	parentLink, err := d.getLink(ctx, parentLinkID)
	if err != nil {
		return "", fmt.Errorf("failed to get parent link: %w", err)
	}

	// Get parent node keyring
	parentNodeKR, err := d.getLinkKR(ctx, parentLink)
	if err != nil {
		return "", fmt.Errorf("failed to get parent keyring: %w", err)
	}

	// Temporary file (request)
	tempReq := proton.CreateFileReq{
		SignatureAddress: d.MainShare.Creator,
	}

	// Encrypt the filename
	err = tempReq.SetName(name, d.DefaultAddrKR, parentNodeKR)
	if err != nil {
		return "", fmt.Errorf("failed to encrypt filename: %w", err)
	}

	return tempReq.Name, nil
}

func (d *ProtonDrive) generateFileNameHash(ctx context.Context, name string, parentLinkID string) (string, error) {
	parentLink, err := d.getLink(ctx, parentLinkID)
	if err != nil {
		return "", fmt.Errorf("failed to get parent link: %w", err)
	}

	// Get parent node keyring
	parentNodeKR, err := d.getLinkKR(ctx, parentLink)
	if err != nil {
		return "", fmt.Errorf("failed to get parent keyring: %w", err)
	}

	signatureVerificationKR, err := d.getSignatureVerificationKeyring([]string{parentLink.SignatureEmail}, parentNodeKR)
	if err != nil {
		return "", fmt.Errorf("failed to get signature verification keyring: %w", err)
	}

	parentHashKey, err := parentLink.GetHashKey(parentNodeKR, signatureVerificationKR)
	if err != nil {
		return "", fmt.Errorf("failed to get parent hash key: %w", err)
	}

	nameHash, err := proton.GetNameHash(name, parentHashKey)
	if err != nil {
		return "", fmt.Errorf("failed to generate name hash: %w", err)
	}

	return nameHash, nil
}

func (d *ProtonDrive) getOriginalNameHash(link *proton.Link) (string, error) {
	if link == nil {
		return "", fmt.Errorf("link cannot be nil")
	}

	if link.Hash == "" {
		return "", fmt.Errorf("link hash is empty")
	}

	return link.Hash, nil
}

func (d *ProtonDrive) getLink(ctx context.Context, linkID string) (*proton.Link, error) {
	if linkID == "" {
		return nil, fmt.Errorf("linkID cannot be empty")
	}

	link, err := d.c.GetLink(ctx, d.MainShare.ShareID, linkID)
	if err != nil {
		return nil, err
	}

	return &link, nil
}

func (d *ProtonDrive) getLinkKR(ctx context.Context, link *proton.Link) (*crypto.KeyRing, error) {
	if link == nil {
		return nil, fmt.Errorf("link cannot be nil")
	}

	// Root Link or Root Dir
	if link.ParentLinkID == "" {
		signatureVerificationKR, err := d.getSignatureVerificationKeyring([]string{link.SignatureEmail})
		if err != nil {
			return nil, err
		}
		return link.GetKeyRing(d.MainShareKR, signatureVerificationKR)
	}

	// Get parent keyring recursively
	parentLink, err := d.getLink(ctx, link.ParentLinkID)
	if err != nil {
		return nil, err
	}

	parentNodeKR, err := d.getLinkKR(ctx, parentLink)
	if err != nil {
		return nil, err
	}

	signatureVerificationKR, err := d.getSignatureVerificationKeyring([]string{link.SignatureEmail})
	if err != nil {
		return nil, err
	}

	return link.GetKeyRing(parentNodeKR, signatureVerificationKR)
}

var (
	ErrKeyPassOrSaltedKeyPassMustBeNotNil = errors.New("either keyPass or saltedKeyPass must be not nil")
	ErrFailedToUnlockUserKeys             = errors.New("failed to unlock user keys")
)

func getAccountKRs(ctx context.Context, c *proton.Client, keyPass, saltedKeyPass []byte) (*crypto.KeyRing, map[string]*crypto.KeyRing, map[string]proton.Address, []byte, error) {
	user, err := c.GetUser(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	// fmt.Printf("user %#v", user)

	addrsArr, err := c.GetAddresses(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	// fmt.Printf("addr %#v", addr)

	if saltedKeyPass == nil {
		if keyPass == nil {
			return nil, nil, nil, nil, ErrKeyPassOrSaltedKeyPassMustBeNotNil
		}

		// Due to limitations, salts are stored using cacheCredentialToFile
		salts, err := c.GetSalts(ctx)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		// fmt.Printf("salts %#v", salts)

		saltedKeyPass, err = salts.SaltForKey(keyPass, user.Keys.Primary().ID)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		// fmt.Printf("saltedKeyPass ok")
	}

	userKR, addrKRs, err := proton.Unlock(user, addrsArr, saltedKeyPass, nil)
	if err != nil {
		return nil, nil, nil, nil, err
	} else if userKR.CountDecryptionEntities() == 0 {
		return nil, nil, nil, nil, ErrFailedToUnlockUserKeys
	}

	addrs := make(map[string]proton.Address)
	for _, addr := range addrsArr {
		addrs[addr.Email] = addr
	}

	return userKR, addrKRs, addrs, saltedKeyPass, nil
}

func (d *ProtonDrive) getSignatureVerificationKeyring(emailAddresses []string, verificationAddrKRs ...*crypto.KeyRing) (*crypto.KeyRing, error) {
	ret, err := crypto.NewKeyRing(nil)
	if err != nil {
		return nil, err
	}

	for _, emailAddress := range emailAddresses {
		if addr, ok := d.addrData[emailAddress]; ok {
			if addrKR, exists := d.addrKRs[addr.ID]; exists {
				err = d.addKeysFromKR(ret, addrKR)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	for _, kr := range verificationAddrKRs {
		err = d.addKeysFromKR(ret, kr)
		if err != nil {
			return nil, err
		}
	}

	if ret.CountEntities() == 0 {
		return nil, fmt.Errorf("no keyring for signature verification")
	}

	return ret, nil
}

func (d *ProtonDrive) addKeysFromKR(kr *crypto.KeyRing, newKRs ...*crypto.KeyRing) error {
	for i := range newKRs {
		for _, key := range newKRs[i].GetKeys() {
			err := kr.AddKey(key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *ProtonDrive) DirectRename(ctx context.Context, srcObj model.Obj, newName string) (model.Obj, error) {
	// fmt.Printf("DEBUG DirectRename: path=%s, newName=%s", srcObj.GetPath(), newName)

	if d.MainShare == nil || d.DefaultAddrKR == nil {
		return nil, fmt.Errorf("missing required fields: MainShare=%v, DefaultAddrKR=%v",
			d.MainShare != nil, d.DefaultAddrKR != nil)
	}

	if d.protonDrive == nil {
		return nil, fmt.Errorf("protonDrive bridge is nil")
	}

	srcLink, err := d.getLink(ctx, srcObj.GetID())
	if err != nil {
		return nil, fmt.Errorf("failed to find source: %w", err)
	}

	parentLinkID := srcLink.ParentLinkID
	if parentLinkID == "" {
		return nil, fmt.Errorf("cannot rename root folder")
	}

	encryptedName, err := d.encryptFileName(ctx, newName, parentLinkID)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt filename: %w", err)
	}

	newHash, err := d.generateFileNameHash(ctx, newName, parentLinkID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate new hash: %w", err)
	}

	originalHash, err := d.getOriginalNameHash(srcLink)
	if err != nil {
		return nil, fmt.Errorf("failed to get original hash: %w", err)
	}

	renameReq := RenameRequest{
		Name:               encryptedName,
		NameSignatureEmail: d.MainShare.Creator,
		Hash:               newHash,
		OriginalHash:       originalHash,
	}

	err = d.executeRenameAPI(ctx, srcLink.LinkID, renameReq)
	if err != nil {
		return nil, fmt.Errorf("rename API call failed: %w", err)
	}

	return &model.Object{
		ID:       srcLink.LinkID,
		Name:     newName,
		Size:     srcObj.GetSize(),
		Modified: srcObj.ModTime(),
		IsFolder: srcObj.IsDir(),
	}, nil
}

func (d *ProtonDrive) executeRenameAPI(ctx context.Context, linkID string, req RenameRequest) error {
	renameURL := fmt.Sprintf(d.apiBase+"/drive/v2/volumes/%s/links/%s/rename",
		d.MainShare.VolumeID, linkID)

	reqBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal rename request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "PUT", renameURL, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", d.protonJson)
	httpReq.Header.Set("X-Pm-Appversion", d.webDriveAV)
	httpReq.Header.Set("X-Pm-Drive-Sdk-Version", d.sdkVersion)
	httpReq.Header.Set("X-Pm-Uid", d.ReusableCredential.UID)
	httpReq.Header.Set("Authorization", "Bearer "+d.ReusableCredential.AccessToken)

	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to execute rename request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rename failed with status %d", resp.StatusCode)
	}

	var renameResp RenameResponse
	if err := json.NewDecoder(resp.Body).Decode(&renameResp); err != nil {
		return fmt.Errorf("failed to decode rename response: %w", err)
	}

	if renameResp.Code != 1000 {
		return fmt.Errorf("rename failed with code %d", renameResp.Code)
	}

	return nil
}

func (d *ProtonDrive) executeMoveAPI(ctx context.Context, linkID string, req MoveRequest) error {
	// fmt.Printf("DEBUG Move Request - Name: %s\n", req.Name)
	// fmt.Printf("DEBUG Move Request - Hash: %s\n", req.Hash)
	// fmt.Printf("DEBUG Move Request - OriginalHash: %s\n", req.OriginalHash)
	// fmt.Printf("DEBUG Move Request - ParentLinkID: %s\n", req.ParentLinkID)

	// fmt.Printf("DEBUG Move Request - Name length: %d\n", len(req.Name))
	// fmt.Printf("DEBUG Move Request - NameSignatureEmail: %s\n", req.NameSignatureEmail)
	// fmt.Printf("DEBUG Move Request - ContentHash: %v\n", req.ContentHash)
	// fmt.Printf("DEBUG Move Request - NodePassphrase length: %d\n", len(req.NodePassphrase))
	// fmt.Printf("DEBUG Move Request - NodePassphraseSignature length: %d\n", len(req.NodePassphraseSignature))

	// fmt.Printf("DEBUG Move Request - SrcLinkID: %s\n", linkID)
	// fmt.Printf("DEBUG Move Request - DstParentLinkID: %s\n", req.ParentLinkID)
	// fmt.Printf("DEBUG Move Request - ShareID: %s\n", d.MainShare.ShareID)

	srcLink, _ := d.getLink(ctx, linkID)
	if srcLink != nil && srcLink.ParentLinkID == req.ParentLinkID {
		return fmt.Errorf("cannot move to same parent directory")
	}

	moveURL := fmt.Sprintf(d.apiBase+"/drive/v2/volumes/%s/links/%s/move",
		d.MainShare.VolumeID, linkID)

	reqBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal move request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "PUT", moveURL, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Authorization", "Bearer "+d.ReusableCredential.AccessToken)
	httpReq.Header.Set("Accept", d.protonJson)
	httpReq.Header.Set("X-Pm-Appversion", d.webDriveAV)
	httpReq.Header.Set("X-Pm-Drive-Sdk-Version", d.sdkVersion)
	httpReq.Header.Set("X-Pm-Uid", d.ReusableCredential.UID)
	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to execute move request: %w", err)
	}
	defer resp.Body.Close()

	var moveResp RenameResponse
	if err := json.NewDecoder(resp.Body).Decode(&moveResp); err != nil {
		return fmt.Errorf("failed to decode move response: %w", err)
	}

	if moveResp.Code != 1000 {
		return fmt.Errorf("move operation failed with code: %d", moveResp.Code)
	}

	return nil
}

func (d *ProtonDrive) DirectMove(ctx context.Context, srcObj model.Obj, dstDir model.Obj) (model.Obj, error) {
	// fmt.Printf("DEBUG DirectMove: srcPath=%s, dstPath=%s", srcObj.GetPath(), dstDir.GetPath())

	srcLink, err := d.getLink(ctx, srcObj.GetID())
	if err != nil {
		return nil, fmt.Errorf("failed to find source: %w", err)
	}

	dstParentLinkID := dstDir.GetID()

	if srcObj.IsDir() {
		// Check if destination is a descendant of source
		if err := d.checkCircularMove(ctx, srcLink.LinkID, dstParentLinkID); err != nil {
			return nil, err
		}
	}

	// Encrypt the filename for the new location
	encryptedName, err := d.encryptFileName(ctx, srcObj.GetName(), dstParentLinkID)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt filename: %w", err)
	}

	newHash, err := d.generateNameHash(ctx, srcObj.GetName(), dstParentLinkID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate new hash: %w", err)
	}

	originalHash, err := d.getOriginalNameHash(srcLink)
	if err != nil {
		return nil, fmt.Errorf("failed to get original hash: %w", err)
	}

	// Re-encrypt node passphrase for new parent context
	reencryptedPassphrase, err := d.reencryptNodePassphrase(ctx, srcLink, dstParentLinkID)
	if err != nil {
		return nil, fmt.Errorf("failed to re-encrypt node passphrase: %w", err)
	}

	moveReq := MoveRequest{
		ParentLinkID:       dstParentLinkID,
		NodePassphrase:     reencryptedPassphrase,
		Name:               encryptedName,
		NameSignatureEmail: d.MainShare.Creator,
		Hash:               newHash,
		OriginalHash:       originalHash,
		ContentHash:        nil,

		// *** Causes rejection ***
		/* NodePassphraseSignature: srcLink.NodePassphraseSignature, */
	}

	//fmt.Printf("DEBUG MoveRequest validation:\n")
	//fmt.Printf("  Name length: %d\n", len(moveReq.Name))
	//fmt.Printf("  Hash: %s\n", moveReq.Hash)
	//fmt.Printf("  OriginalHash: %s\n", moveReq.OriginalHash)
	//fmt.Printf("  NodePassphrase length: %d\n", len(moveReq.NodePassphrase))
	/* fmt.Printf("  NodePassphraseSignature length: %d\n", len(moveReq.NodePassphraseSignature)) */
	//fmt.Printf("  NameSignatureEmail: %s\n", moveReq.NameSignatureEmail)

	err = d.executeMoveAPI(ctx, srcLink.LinkID, moveReq)
	if err != nil {
		return nil, fmt.Errorf("move API call failed: %w", err)
	}

	return &model.Object{
		ID:       srcLink.LinkID,
		Name:     srcObj.GetName(),
		Size:     srcObj.GetSize(),
		Modified: srcObj.ModTime(),
		IsFolder: srcObj.IsDir(),
	}, nil
}

func (d *ProtonDrive) reencryptNodePassphrase(ctx context.Context, srcLink *proton.Link, dstParentLinkID string) (string, error) {
	// Get source parent link with metadata
	srcParentLink, err := d.getLink(ctx, srcLink.ParentLinkID)
	if err != nil {
		return "", fmt.Errorf("failed to get source parent link: %w", err)
	}

	// Get source parent keyring using link object
	srcParentKR, err := d.getLinkKR(ctx, srcParentLink)
	if err != nil {
		return "", fmt.Errorf("failed to get source parent keyring: %w", err)
	}

	// Get destination parent link with metadata
	dstParentLink, err := d.getLink(ctx, dstParentLinkID)
	if err != nil {
		return "", fmt.Errorf("failed to get destination parent link: %w", err)
	}

	// Get destination parent keyring using link object
	dstParentKR, err := d.getLinkKR(ctx, dstParentLink)
	if err != nil {
		return "", fmt.Errorf("failed to get destination parent keyring: %w", err)
	}

	// Re-encrypt the node passphrase from source parent context to destination parent context
	reencryptedPassphrase, err := reencryptKeyPacket(srcParentKR, dstParentKR, d.DefaultAddrKR, srcLink.NodePassphrase)
	if err != nil {
		return "", fmt.Errorf("failed to re-encrypt key packet: %w", err)
	}

	return reencryptedPassphrase, nil
}

func (d *ProtonDrive) generateNameHash(ctx context.Context, name string, parentLinkID string) (string, error) {
	parentLink, err := d.getLink(ctx, parentLinkID)
	if err != nil {
		return "", fmt.Errorf("failed to get parent link: %w", err)
	}

	// Get parent node keyring
	parentNodeKR, err := d.getLinkKR(ctx, parentLink)
	if err != nil {
		return "", fmt.Errorf("failed to get parent keyring: %w", err)
	}

	// Get signature verification keyring
	signatureVerificationKR, err := d.getSignatureVerificationKeyring([]string{parentLink.SignatureEmail}, parentNodeKR)
	if err != nil {
		return "", fmt.Errorf("failed to get signature verification keyring: %w", err)
	}

	parentHashKey, err := parentLink.GetHashKey(parentNodeKR, signatureVerificationKR)
	if err != nil {
		return "", fmt.Errorf("failed to get parent hash key: %w", err)
	}

	nameHash, err := proton.GetNameHash(name, parentHashKey)
	if err != nil {
		return "", fmt.Errorf("failed to generate name hash: %w", err)
	}

	return nameHash, nil
}

func reencryptKeyPacket(srcKR, dstKR, _ *crypto.KeyRing, passphrase string) (string, error) { // addrKR (3)
	oldSplitMessage, err := crypto.NewPGPSplitMessageFromArmored(passphrase)
	if err != nil {
		return "", err
	}

	sessionKey, err := srcKR.DecryptSessionKey(oldSplitMessage.KeyPacket)
	if err != nil {
		return "", err
	}

	newKeyPacket, err := dstKR.EncryptSessionKey(sessionKey)
	if err != nil {
		return "", err
	}

	newSplitMessage := crypto.NewPGPSplitMessage(newKeyPacket, oldSplitMessage.DataPacket)

	return newSplitMessage.GetArmored()
}

func (d *ProtonDrive) checkCircularMove(ctx context.Context, srcLinkID, dstParentLinkID string) error {
	currentLinkID := dstParentLinkID

	for currentLinkID != "" && currentLinkID != d.RootFolderID {
		if currentLinkID == srcLinkID {
			return fmt.Errorf("cannot move folder into itself or its subfolder")
		}

		currentLink, err := d.getLink(ctx, currentLinkID)
		if err != nil {
			return err
		}
		currentLinkID = currentLink.ParentLinkID
	}

	return nil
}

func (d *ProtonDrive) authHandler(auth proton.Auth) {
	if auth.AccessToken != d.ReusableCredential.AccessToken || auth.RefreshToken != d.ReusableCredential.RefreshToken {
		d.ReusableCredential.UID = auth.UID
		d.ReusableCredential.AccessToken = auth.AccessToken
		d.ReusableCredential.RefreshToken = auth.RefreshToken

		if err := d.initClient(context.Background()); err != nil {
			fmt.Printf("ProtonDrive: failed to reinitialize client after auth refresh: %v\n", err)
		}

		op.MustSaveDriverStorage(d)
	}
}

func (d *ProtonDrive) initClient(ctx context.Context) error {
	clientOptions := []proton.Option{
		proton.WithAppVersion(d.appVersion),
		proton.WithUserAgent(d.userAgent),
	}
	manager := proton.New(clientOptions...)
	d.c = manager.NewClient(d.ReusableCredential.UID, d.ReusableCredential.AccessToken, d.ReusableCredential.RefreshToken)

	saltedKeyPassBytes, err := base64.StdEncoding.DecodeString(d.ReusableCredential.SaltedKeyPass)
	if err != nil {
		return fmt.Errorf("failed to decode salted key pass: %w", err)
	}

	_, addrKRs, addrs, _, err := getAccountKRs(ctx, d.c, nil, saltedKeyPassBytes)
	if err != nil {
		return fmt.Errorf("failed to get account keyrings: %w", err)
	}

	d.addrKRs = addrKRs
	d.addrData = addrs

	return nil
}
