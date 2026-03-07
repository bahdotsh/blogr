/**
 * Blogr Search - Client-side search functionality using MiniSearch
 * 
 * This script provides full-text search capabilities for Blogr sites.
 * It loads the search index and provides real-time search functionality.
 */

class BlogrSearch {
    constructor(options = {}) {
        this.options = {
            searchInput: '#search-input',
            resultsContainer: '#search-results',
            indexUrl: 'search_index.json',
            maxResults: 10,
            showMoreStep: 10,
            minQueryLength: 2,
            debounceMs: 300,
            lazyLoad: true,
            miniSearchUrl: 'js/vendor/minisearch.min.js',
            ...options
        };
        
        this.miniSearch = null;
        this.indexData = null;
        this.isInitialized = false;
        this.isLoading = false;
        this.debounceTimer = null;
        this.baseHref = '/';
        this.currentResults = [];
        this.currentLimit = this.options.maxResults;
        
        this.init();
    }
    
    async init() {
        try {
            if (this.options.lazyLoad) {
                // For lazy loading, just set up event listeners
                // The search index will be loaded on first search
                this.setupEventListeners();
                console.log('🔍 Blogr search initialized (lazy loading)');
            } else {
                await this.loadSearchIndex();
                this.setupEventListeners();
                this.isInitialized = true;
                console.log('🔍 Blogr search initialized');
            }
        } catch (error) {
            console.error('Failed to initialize search:', error);
        }
    }
    
    async loadSearchIndex() {
        if (this.isInitialized || this.isLoading) {
            return;
        }
        
        this.isLoading = true;
        
        try {
            // Load MiniSearch if not already available
            if (typeof MiniSearch === 'undefined') {
                await this.loadMiniSearch();
            }
            
            const baseEl = document.querySelector('meta[name="blogr-base"]');
            this.baseHref = baseEl ? baseEl.getAttribute('content') || '/' : '/';
            const indexPath = this.joinUrl(this.baseHref, this.options.indexUrl);
            const response = await fetch(indexPath);
            if (!response.ok) {
                throw new Error(`Failed to load search index: ${response.status}`);
            }
            
            this.indexData = await response.json();
            
            // Initialize MiniSearch with the loaded data
            this.miniSearch = new MiniSearch({
                fields: ['title', 'tags', 'content'],
                storeFields: ['title', 'url', 'date', 'tags', 'excerpt', 'description'],
                searchOptions: {
                    boost: {"title":5.0,"tags":3.0,"content":1.0},
                    prefix: true,
                    fuzzy: 0.2
                }
            });
            
            this.miniSearch.addAll(this.indexData);
            this.isInitialized = true;
            console.log(`📚 Loaded ${this.indexData.length} documents for search`);
        } catch (error) {
            console.error('Error loading search index:', error);
            throw error;
        } finally {
            this.isLoading = false;
        }
    }
    
    async loadMiniSearch() {
        return new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = this.joinUrl(this.baseHref, this.options.miniSearchUrl);
            script.onload = () => {
                console.log('📦 MiniSearch library loaded');
                resolve();
            };
            script.onerror = () => {
                reject(new Error('Failed to load MiniSearch library'));
            };
            document.head.appendChild(script);
        });
    }
    
    setupEventListeners() {
        const searchInput = document.querySelector(this.options.searchInput);
        const resultsContainer = document.querySelector(this.options.resultsContainer);
        
        if (!searchInput || !resultsContainer) {
            console.warn('Search input or results container not found');
            return;
        }
        
        // Handle input events with debouncing
        searchInput.addEventListener('input', (e) => {
            this.handleSearch(e.target.value);
        });
        
        // Handle form submission
        const form = searchInput.closest('form');
        if (form) {
            form.addEventListener('submit', (e) => {
                e.preventDefault();
                this.handleSearch(searchInput.value);
            });
        }
        
        // Keyboard UX: '/' focuses search, Escape clears/hides
        document.addEventListener('keydown', (e) => {
            if (e.key === '/') {
                const active = document.activeElement;
                const isTyping = active && (active.tagName === 'INPUT' || active.tagName === 'TEXTAREA' || active.isContentEditable);
                if (!isTyping) {
                    e.preventDefault();
                    searchInput.focus();
                }
            } else if (e.key === 'Escape') {
                this.clearSearch();
                resultsContainer.innerHTML = '';
            } else if (e.key === 'ArrowDown') {
                this.moveSelection(1);
            } else if (e.key === 'ArrowUp') {
                this.moveSelection(-1);
            } else if (e.key === 'Enter') {
                const selected = resultsContainer.querySelector('.search-result-item.is-active a.search-result-link');
                if (selected) {
                    window.location.href = selected.getAttribute('href');
                }
            }
        });
        
        // Handle clicks outside to hide results
        document.addEventListener('click', (e) => {
            if (!searchInput.contains(e.target) && !resultsContainer.contains(e.target)) {
                this.hideResults();
            }
        });

        // Delegate interactions inside results
        resultsContainer.addEventListener('click', (e) => {
            const moreBtn = e.target.closest('.search-results-more');
            if (moreBtn) {
                e.preventDefault();
                this.currentLimit += this.options.showMoreStep;
                this.renderResults(this.currentResults, searchInput.value.trim());
                return;
            }

            const tagEl = e.target.closest('.search-tag');
            if (tagEl && tagEl.dataset && tagEl.dataset.tag) {
                e.preventDefault();
                const tag = tagEl.dataset.tag;
                searchInput.value = tag;
                this.handleSearch(tag);
                searchInput.focus();
                return;
            }

            const copyBtn = e.target.closest('.search-action-copy');
            if (copyBtn) {
                e.preventDefault();
                const item = copyBtn.closest('.search-result-item');
                const link = item && item.querySelector('a.search-result-link');
                if (link) {
                    const href = link.getAttribute('href');
                    const abs = this.joinUrl(location.origin, href.replace(location.origin, ''));
                    if (navigator.clipboard && navigator.clipboard.writeText) {
                        navigator.clipboard.writeText(abs).then(() => {
                            copyBtn.classList.add('copied');
                            setTimeout(() => copyBtn.classList.remove('copied'), 1000);
                        }).catch(() => {});
                    }
                }
                return;
            }

            const newTabBtn = e.target.closest('.search-action-newtab');
            if (newTabBtn) {
                e.preventDefault();
                const item = newTabBtn.closest('.search-result-item');
                const link = item && item.querySelector('a.search-result-link');
                if (link) {
                    window.open(link.getAttribute('href'), '_blank');
                }
                return;
            }

            const openBtn = e.target.closest('.search-action-open');
            if (openBtn) {
                e.preventDefault();
                const item = openBtn.closest('.search-result-item');
                const link = item && item.querySelector('a.search-result-link');
                if (link) {
                    window.location.href = link.getAttribute('href');
                }
                return;
            }

            // Click anywhere on item navigates
            const item = e.target.closest('.search-result-item');
            if (item && resultsContainer.contains(item)) {
                const link = item.querySelector('a.search-result-link');
                if (link) {
                    window.location.href = link.getAttribute('href');
                }
            }
        });
    }
    
    handleSearch(query) {
        // Clear previous debounce timer
        if (this.debounceTimer) {
            clearTimeout(this.debounceTimer);
        }
        
        // Debounce the search
        this.debounceTimer = setTimeout(() => {
            this.performSearch(query);
        }, this.options.debounceMs);
    }
    
    async performSearch(query) {
        const trimmedQuery = query.trim();
        
        if (trimmedQuery.length < this.options.minQueryLength) {
            this.hideResults();
            return;
        }
        
        // Lazy load search index if not initialized
        if (!this.isInitialized && this.options.lazyLoad) {
            if (this.isLoading) {
                this.showLoading();
                return;
            }
            
            try {
                this.showLoading();
                await this.loadSearchIndex();
            } catch (error) {
                console.error('Failed to load search:', error);
                this.showError('Failed to load search. Please try again.');
                return;
            }
        }
        
        if (!this.isInitialized || !this.miniSearch) {
            return;
        }
        
        try {
            const results = this.miniSearch.search(trimmedQuery, {
                prefix: true,
                fuzzy: 0.2,
                boost: {"title":5.0,"tags":3.0,"content":1.0}
            }).slice(0, this.options.maxResults);
            // Keep full set for pagination; we will manage limits during render
            this.currentResults = this.miniSearch.search(trimmedQuery, {
                prefix: true,
                fuzzy: 0.2,
                boost: {"title":5.0,"tags":3.0,"content":1.0}
            });
            this.currentLimit = this.options.maxResults;
            this.renderResults(this.currentResults, trimmedQuery);
        } catch (error) {
            console.error('Search error:', error);
            this.showError('Search failed. Please try again.');
        }
    }
    
    renderResults(results, query) {
        const resultsContainer = document.querySelector(this.options.resultsContainer);
        if (!resultsContainer) return;
        
        if (!results || results.length === 0) {
            this.showNoResults(query);
            return;
        }

        const limited = results.slice(0, this.currentLimit);
        const html = limited.map((result, idx) => this.renderResultItem(result, query, idx === 0)).join('');
        const hasMore = results.length > this.currentLimit;
        const footer = hasMore ? `
            <div class="search-results-footer">
                <button type="button" class="search-results-more">Show more results</button>
            </div>
        ` : '';
        resultsContainer.innerHTML = html + footer;
        this.showResults();
    }
    
    renderResultItem(result, query, active = false) {
        const title = this.highlightText(result.title, query);
        const excerpt = this.highlightText(result.excerpt || result.description || '', query);
        const tags = result.tags ? result.tags.map(tag => `<button type="button" class="search-tag" data-tag="${this.escapeHtml(tag)}">${this.escapeHtml(tag)}</button>`).join('') : '';
        const date = result.date ? `<span class="search-date">${result.date}</span>` : '';
        const url = this.joinUrl(this.baseHref, result.url || '');
        
        return `
            <article class="search-result-item${active ? ' is-active' : ''}">
                <h3 class="search-result-title">
                    <a href="${url}" class="search-result-link">${title}</a>
                </h3>
                <div class="search-result-meta">
                    ${date}
                    ${tags ? `<div class="search-result-tags">${tags}</div>` : ''}
                </div>
                <p class="search-result-excerpt">${excerpt}</p>
                <div class="search-result-actions">
                    <button type="button" class="search-action search-action-open" aria-label="Open">Open</button>
                    <button type="button" class="search-action search-action-newtab" aria-label="Open in new tab">New tab</button>
                    <button type="button" class="search-action search-action-copy" aria-label="Copy link">Copy link</button>
                </div>
            </article>
        `;
    }

    moveSelection(delta) {
        const resultsContainer = document.querySelector(this.options.resultsContainer);
        if (!resultsContainer) return;
        const items = Array.from(resultsContainer.querySelectorAll('.search-result-item'));
        if (items.length === 0) return;
        let idx = items.findIndex(el => el.classList.contains('is-active'));
        if (idx === -1) idx = 0;
        items.forEach(el => el.classList.remove('is-active'));
        const next = (idx + delta + items.length) % items.length;
        items[next].classList.add('is-active');
        items[next].scrollIntoView({ block: 'nearest' });
    }

    joinUrl(base, path) {
        // Return absolute URLs as-is
        if (/^https?:\/\//i.test(path)) return path;
        if (!base) return path;
        if (base.endsWith('/') && path.startsWith('/')) return base + path.substring(1);
        if (!base.endsWith('/') && !path.startsWith('/')) return base + '/' + path;
        return base + path;
    }
    
    escapeHtml(str) {
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    highlightText(text, query) {
        if (!query || !text) return text;
        
        const regex = new RegExp(`(${this.escapeRegex(query)})`, 'gi');
        return text.replace(regex, '<mark class="search-highlight">$1</mark>');
    }
    
    escapeRegex(string) {
        return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }
    
    showResults() {
        const resultsContainer = document.querySelector(this.options.resultsContainer);
        if (resultsContainer) {
            resultsContainer.removeAttribute('hidden');
            resultsContainer.style.display = 'block';
            resultsContainer.classList.add('search-results-visible');
        }
    }
    
    hideResults() {
        const resultsContainer = document.querySelector(this.options.resultsContainer);
        if (resultsContainer) {
            resultsContainer.setAttribute('hidden', 'hidden');
            resultsContainer.style.display = 'none';
            resultsContainer.classList.remove('search-results-visible');
        }
    }
    
    showNoResults(query) {
        const resultsContainer = document.querySelector(this.options.resultsContainer);
        if (!resultsContainer) return;
        
        resultsContainer.innerHTML = `
            <div class="search-no-results">
                <p>No results found for "<strong>${query}</strong>"</p>
                <p class="search-suggestions">Try different keywords or check your spelling.</p>
            </div>
        `;
        this.showResults();
    }
    
    showError(message) {
        const resultsContainer = document.querySelector(this.options.resultsContainer);
        if (!resultsContainer) return;
        
        resultsContainer.innerHTML = `
            <div class="search-error">
                <p>${message}</p>
            </div>
        `;
        this.showResults();
    }
    
    showLoading() {
        const resultsContainer = document.querySelector(this.options.resultsContainer);
        if (!resultsContainer) return;
        
        resultsContainer.innerHTML = `
            <div class="search-loading">
                <p>Loading search...</p>
            </div>
        `;
        this.showResults();
    }
    
    clearSearch() {
        const searchInput = document.querySelector(this.options.searchInput);
        if (searchInput) {
            searchInput.value = '';
        }
        this.hideResults();
    }
}

// Auto-initialize when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    // Check if MiniSearch is available
    if (typeof MiniSearch === 'undefined') {
        console.error('MiniSearch is not loaded. Please include the MiniSearch library.');
        return;
    }
    
    // Initialize search if elements are present
    const searchInput = document.querySelector('#search-input');
    const resultsContainer = document.querySelector('#search-results');
    
    if (searchInput && resultsContainer) {
        new BlogrSearch();
    }
});

// Export for manual initialization if needed
window.BlogrSearch = BlogrSearch;
