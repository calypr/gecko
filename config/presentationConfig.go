package config

import (
	"bytes"
	"fmt"
	"net/url"
	"strings"

	"golang.org/x/net/html"
)

type PresentationConfig struct {
	PresentationConfig string `json:"presentationConfig"`
}

func (p PresentationConfig) IsZero() bool {
	return strings.TrimSpace(p.PresentationConfig) == ""
}

func (p *PresentationConfig) Validate() error {
	if p == nil {
		return fmt.Errorf("presentation config is required")
	}
	sanitized, err := sanitizePresentationHTML(p.PresentationConfig)
	if err != nil {
		return err
	}
	p.PresentationConfig = sanitized
	return nil
}

func sanitizePresentationHTML(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil
	}
	if err := validatePresentationHTML(raw); err != nil {
		return "", err
	}
	doc, err := html.Parse(strings.NewReader(raw))
	if err != nil {
		return "", fmt.Errorf("presentationConfig must contain valid HTML: %w", err)
	}

	container := &html.Node{Type: html.ElementNode, Data: "div"}
	source := findHTMLElement(doc, "body")
	if source == nil {
		source = doc
	}
	for child := source.FirstChild; child != nil; child = child.NextSibling {
		sanitizePresentationNode(container, child)
	}

	var buf bytes.Buffer
	for child := container.FirstChild; child != nil; child = child.NextSibling {
		if err := html.Render(&buf, child); err != nil {
			return "", fmt.Errorf("render sanitized presentation HTML: %w", err)
		}
	}
	return strings.TrimSpace(buf.String()), nil
}

func validatePresentationHTML(raw string) error {
	tokenizer := html.NewTokenizer(strings.NewReader(raw))
	var stack []string

	for {
		switch tokenizer.Next() {
		case html.ErrorToken:
			if err := tokenizer.Err(); err != nil {
				if err.Error() == "EOF" {
					if len(stack) != 0 {
						return fmt.Errorf("presentationConfig must contain balanced HTML tags")
					}
					return nil
				}
				return fmt.Errorf("presentationConfig must contain valid HTML: %w", err)
			}
			if len(stack) != 0 {
				return fmt.Errorf("presentationConfig must contain balanced HTML tags")
			}
			return nil
		case html.StartTagToken:
			tagName, hasAttr := tokenizer.TagName()
			tag := strings.ToLower(string(tagName))
			if !presentationVoidElements[tag] {
				stack = append(stack, tag)
			}
			if hasAttr {
				for {
					_, _, more := tokenizer.TagAttr()
					if !more {
						break
					}
				}
			}
		case html.SelfClosingTagToken:
			if _, hasAttr := tokenizer.TagName(); hasAttr {
				for {
					_, _, more := tokenizer.TagAttr()
					if !more {
						break
					}
				}
			}
		case html.EndTagToken:
			tagName, _ := tokenizer.TagName()
			tag := strings.ToLower(string(tagName))
			if presentationVoidElements[tag] {
				return fmt.Errorf("presentationConfig must not close void element <%s>", tag)
			}
			if len(stack) == 0 || stack[len(stack)-1] != tag {
				return fmt.Errorf("presentationConfig must contain balanced HTML tags")
			}
			stack = stack[:len(stack)-1]
		}
	}
}

func sanitizePresentationNode(parent *html.Node, node *html.Node) {
	switch node.Type {
	case html.TextNode:
		parent.AppendChild(&html.Node{Type: html.TextNode, Data: node.Data})
	case html.ElementNode:
		tag := strings.ToLower(node.Data)
		if presentationDropContentTags[tag] {
			return
		}
		if !presentationAllowedTags[tag] {
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				sanitizePresentationNode(parent, child)
			}
			return
		}

		sanitized := &html.Node{Type: html.ElementNode, Data: tag}
		for _, attr := range sanitizePresentationAttrs(tag, node.Attr) {
			sanitized.Attr = append(sanitized.Attr, attr)
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			sanitizePresentationNode(sanitized, child)
		}
		parent.AppendChild(sanitized)
	}
}

func sanitizePresentationAttrs(tag string, attrs []html.Attribute) []html.Attribute {
	sanitized := make([]html.Attribute, 0, len(attrs))
	hasRel := false
	hasHref := false
	targetBlank := false

	for _, attr := range attrs {
		key := strings.ToLower(strings.TrimSpace(attr.Key))
		if !presentationAttrAllowed(tag, key) {
			continue
		}
		value := strings.TrimSpace(attr.Val)
		switch key {
		case "href":
			if !isSafePresentationURL(value, false) {
				continue
			}
			hasHref = true
		case "src":
			if !isSafePresentationURL(value, true) {
				continue
			}
		case "target":
			if value == "_blank" {
				targetBlank = true
			}
		case "rel":
			hasRel = true
		}
		sanitized = append(sanitized, html.Attribute{Key: key, Val: value})
	}

	if tag == "a" && !hasHref {
		filtered := sanitized[:0]
		for _, attr := range sanitized {
			if attr.Key == "target" || attr.Key == "rel" {
				continue
			}
			filtered = append(filtered, attr)
		}
		sanitized = filtered
	} else if tag == "a" && targetBlank && !hasRel {
		sanitized = append(sanitized, html.Attribute{Key: "rel", Val: "noopener noreferrer"})
	}
	return sanitized
}

func presentationAttrAllowed(tag string, key string) bool {
	if presentationGlobalAttrs[key] {
		return true
	}
	if strings.HasPrefix(key, "aria-") || strings.HasPrefix(key, "data-") {
		return true
	}
	if allowed := presentationTagAttrs[tag]; allowed != nil {
		return allowed[key]
	}
	return false
}

func isSafePresentationURL(raw string, allowDataImage bool) bool {
	if raw == "" {
		return true
	}
	lower := strings.ToLower(raw)
	if strings.HasPrefix(lower, "#") || strings.HasPrefix(lower, "/") || strings.HasPrefix(lower, "./") || strings.HasPrefix(lower, "../") {
		return true
	}
	if allowDataImage && strings.HasPrefix(lower, "data:image/") {
		return true
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return false
	}
	if parsed.Scheme == "" {
		return true
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https", "mailto", "tel":
		return true
	default:
		return false
	}
}

func findHTMLElement(node *html.Node, tag string) *html.Node {
	if node == nil {
		return nil
	}
	if node.Type == html.ElementNode && strings.EqualFold(node.Data, tag) {
		return node
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if found := findHTMLElement(child, tag); found != nil {
			return found
		}
	}
	return nil
}

var presentationAllowedTags = map[string]bool{
	"a": true, "article": true, "aside": true, "b": true, "blockquote": true,
	"br": true, "caption": true, "code": true, "dd": true, "div": true,
	"dl": true, "dt": true, "em": true, "figcaption": true, "figure": true,
	"footer": true, "h1": true, "h2": true, "h3": true, "h4": true,
	"h5": true, "h6": true, "header": true, "hr": true, "i": true,
	"img": true, "li": true, "main": true, "mark": true, "ol": true,
	"p": true, "pre": true, "section": true, "small": true, "span": true,
	"strong": true, "sub": true, "sup": true, "table": true, "tbody": true,
	"td": true, "tfoot": true, "th": true, "thead": true, "tr": true,
	"u": true, "ul": true,
}

var presentationDropContentTags = map[string]bool{
	"embed":    true,
	"iframe":   true,
	"link":     true,
	"meta":     true,
	"noscript": true,
	"object":   true,
	"script":   true,
	"style":    true,
}

var presentationVoidElements = map[string]bool{
	"area": true, "base": true, "br": true, "col": true, "embed": true,
	"hr": true, "img": true, "input": true, "link": true, "meta": true,
	"param": true, "source": true, "track": true, "wbr": true,
}

var presentationGlobalAttrs = map[string]bool{
	"class": true,
	"dir":   true,
	"id":    true,
	"lang":  true,
	"role":  true,
	"title": true,
}

var presentationTagAttrs = map[string]map[string]bool{
	"a": {
		"href":   true,
		"rel":    true,
		"target": true,
	},
	"img": {
		"alt":    true,
		"height": true,
		"src":    true,
		"width":  true,
	},
	"td": {
		"colspan": true,
		"rowspan": true,
	},
	"th": {
		"colspan": true,
		"rowspan": true,
		"scope":   true,
	},
}
