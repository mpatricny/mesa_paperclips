(function (window) {
  const MESA_VERSION = '1.1.0';
  const REQUEST_TIMEOUT = 5000;

  // Error Codes
  const ErrorCode = {
    TIMEOUT: 'timeout',
    QUOTA_EXCEEDED: 'quota_exceeded',
    NOT_INITIALIZED: 'not_initialized',
    INVALID_INPUT: 'invalid_input',
    RATE_LIMITED: 'rate_limited',
    NETWORK_ERROR: 'network_error',
    NOT_FOUND: 'not_found',
    UNAUTHORIZED: 'unauthorized',
    NOT_LOGGED_IN: 'not_logged_in'
  };

  // Browser language code to stnadard locale code mapping
  const langCodeMap = {
    'en': 'en_us',
    'es': 'es_es',
    'fr': 'fr_fr',
    'de': 'de_de',
    'it': 'it_it',
    'pt': 'pt_pt',
    'ru': 'ru_ru',
    'ja': 'ja_jp',
    'ko': 'ko_kr',
    'zh': 'zh_cn',
    'hi': 'hi_in',
    'ar': 'ar_sa',
    'nl': 'nl_nl',
    'pl': 'pl_pl',
    'tr': 'tr_tr',
    'vi': 'vi_vn',
    'th': 'th_th',
    'sv': 'sv_se',
    'cs': 'cs_cz',
    'el': 'el_gr',
    'hu': 'hu_hu',
    'ro': 'ro_ro',
    'uk': 'uk_ua',
    'id': 'id_id',
    'ms': 'ms_my',
    'tl': 'tl_ph',
    'fil': 'tl_ph',
    'bn': 'bn_bd',
    'ta': 'ta_in',
    'fa': 'fa_ir'
  };

  // Internal state
  let environment = 'disabled'; // 'mesa', 'local', 'disabled'
  let currentUser = null;
  let currentConfig = null;
  let sessionNonce = null;
  let isInitialized = false;
  let initPromiseResolve = null;
  let pendingRequests = new Map(); // requestId -> { resolve, reject, timer }
  let eventListeners = new Map(); // eventName -> [callbacks]

  // Local leaderboard storage for local mode
  let localLeaderboards = {};

  // Helper to generate IDs
  function generateId() {
    return Math.random().toString(36).substring(2, 15);
  }

  // Map browser language to our supported locales
  function mapBrowserLanguage(langCode) {
    if (!langCode) return null;
    const code = langCode.toLowerCase().split('-')[0];
    const fullCode = langCode.toLowerCase();
    // Try full code first (e.g. zh-hant), then short code (e.g. en)
    return langCodeMap[fullCode] || langCodeMap[code] || null;
  }

  // Get browser languages as array
  function getBrowserLanguages() {
    const langs = navigator.languages || [navigator.language];
    return langs.map(mapBrowserLanguage).filter(Boolean);
  }

  // Get preferred locales (internal helper)
  function getUserLocales() {
    const result = [];
    const seen = new Set();
    
    // Add user profile languages first
    if (currentUser && currentUser.languages) {
      if (currentUser.languages.primary && !seen.has(currentUser.languages.primary)) {
        result.push(currentUser.languages.primary);
        seen.add(currentUser.languages.primary);
      }
      if (currentUser.languages.secondary && !seen.has(currentUser.languages.secondary)) {
        result.push(currentUser.languages.secondary);
        seen.add(currentUser.languages.secondary);
      }
    }
    
    // Add browser detected languages
    const browserLangs = getBrowserLanguages();
    for (const lang of browserLangs) {
      if (!seen.has(lang)) {
        result.push(lang);
        seen.add(lang);
      }
    }
    
    return result;
  }

  // Emit event to listeners
  function emit(eventName, data) {
    const listeners = eventListeners.get(eventName) || [];
    listeners.forEach(cb => {
      try {
        cb(data);
      } catch (e) {
        console.error('Mesa SDK: Error in event listener', e);
      }
    });
  }

  // Transport Layer
  const Transport = {
    send: function (type, payload = {}) {
      if (environment === 'local') {
        // Simulate async response for local mode
        setTimeout(() => handleLocalRequest(type, payload), 50);
        return;
      }

      if (window.parent === window) {
        console.warn('Mesa SDK: Not in an iframe, cannot send message.');
        return;
      }

      const message = {
        source: 'mesa-sdk',
        type: type,
        nonce: sessionNonce,
        ...payload
      };
      window.parent.postMessage(message, '*'); // In production, targetOrigin should be locked down
    }
  };

  // Local Storage Simulation
  function handleLocalRequest(type, payload) {
    const { requestId } = payload;
    
    switch (type) {
      case 'mesa:ready':
        // Simulate init response with local languages
        receiveMessage({
          data: {
            type: 'mesa:init',
            user: { 
              id: 'local-user', 
              username: 'LocalDev', 
              avatar: null,
              languages: { primary: null, secondary: null }
            },
            config: { 
              env: 'local',
              resolution: { width: 1920, height: 1080 }
            },
            nonce: 'local-nonce-' + generateId()
          }
        });
        break;
        
      case 'mesa:data:set':
        localStorage.setItem(`mesa_local_${payload.key}`, payload.value);
        if (requestId) {
          receiveMessage({
            data: { type: 'mesa:data:response', requestId, success: true }
          });
        }
        break;

      case 'mesa:data:get':
        const value = localStorage.getItem(`mesa_local_${payload.key}`);
        if (requestId) {
          receiveMessage({
            data: { type: 'mesa:data:response', requestId, value }
          });
        }
        break;

      case 'mesa:data:remove':
        localStorage.removeItem(`mesa_local_${payload.key}`);
        if (requestId) {
          receiveMessage({
            data: { type: 'mesa:data:response', requestId, success: true }
          });
        }
        break;
        
      case 'mesa:data:clear':
        // Only clear mesa keys
        Object.keys(localStorage).forEach(k => {
          if (k.startsWith('mesa_local_')) localStorage.removeItem(k);
        });
        if (requestId) {
          receiveMessage({
            data: { type: 'mesa:data:response', requestId, success: true }
          });
        }
        break;

      case 'mesa:leaderboard:submit':
        handleLocalLeaderboardSubmit(payload, requestId);
        break;

      case 'mesa:leaderboard:get':
        handleLocalLeaderboardGet(payload, requestId);
        break;

      case 'mesa:leaderboard:getTop':
        handleLocalLeaderboardGetTop(payload, requestId);
        break;
    }
  }

  // Local leaderboard handlers
  function handleLocalLeaderboardSubmit(payload, requestId) {
    const { key = 'default', playerName, displayValue, sortValue } = payload;
    const storageKey = `mesa_local_leaderboard_${key}`;
    
    // Load existing leaderboard
    let board = [];
    try {
      const stored = localStorage.getItem(storageKey);
      if (stored) board = JSON.parse(stored);
    } catch (e) {}

    // Find existing entry for this user
    const userId = currentUser ? currentUser.id : 'local-user';
    const existingIdx = board.findIndex(e => e.userId === userId);
    
    let isNewBest = false;
    if (existingIdx >= 0) {
      // Update only if better
      if (sortValue > board[existingIdx].sortValue) {
        board[existingIdx] = { userId, playerName, displayValue, sortValue, submittedAt: Date.now() };
        isNewBest = true;
      }
    } else {
      // New entry
      board.push({ userId, playerName, displayValue, sortValue, submittedAt: Date.now() });
      isNewBest = true;
    }

    // Sort by sortValue DESC
    board.sort((a, b) => b.sortValue - a.sortValue);

    // Limit to 1000 entries
    const MAX_ENTRIES = 1000;
    if (board.length > MAX_ENTRIES) board = board.slice(0, MAX_ENTRIES);

    // Save
    localStorage.setItem(storageKey, JSON.stringify(board));

    // Find rank
    const playerIdx = board.findIndex(e => e.userId === userId);
    const rank = playerIdx >= 0 ? playerIdx + 1 : null;
    const onLeaderboard = playerIdx >= 0;

    if (requestId) {
      receiveMessage({
        data: { type: 'mesa:leaderboard:response', requestId, success: true, rank, isNewBest, onLeaderboard }
      });
    }
  }

  function handleLocalLeaderboardGet(payload, requestId) {
    const { key = 'default' } = payload;
    const storageKey = `mesa_local_leaderboard_${key}`;
    
    let board = [];
    try {
      const stored = localStorage.getItem(storageKey);
      if (stored) board = JSON.parse(stored);
    } catch (e) {}

    const userId = currentUser ? currentUser.id : 'local-user';
    const playerIdx = board.findIndex(e => e.userId === userId);
    const playerRank = playerIdx >= 0 ? playerIdx + 1 : null;

    // Get 50 entries centered around player (or top 50 if not ranked)
    let startIdx = 0;
    if (playerIdx >= 0) {
      startIdx = Math.max(0, playerIdx - 25);
      // Adjust to always get 50 if possible
      if (startIdx + 50 > board.length) {
        startIdx = Math.max(0, board.length - 50);
      }
    }

    const entries = board.slice(startIdx, startIdx + 50).map((e, i) => ({
      rank: startIdx + i + 1,
      playerName: e.playerName,
      displayValue: e.displayValue,
      isCurrentUser: e.userId === userId
    }));

    if (requestId) {
      receiveMessage({
        data: { type: 'mesa:leaderboard:response', requestId, playerRank, entries }
      });
    }
  }

  function handleLocalLeaderboardGetTop(payload, requestId) {
    const { key = 'default', limit = 10 } = payload;
    const storageKey = `mesa_local_leaderboard_${key}`;
    
    let board = [];
    try {
      const stored = localStorage.getItem(storageKey);
      if (stored) board = JSON.parse(stored);
    } catch (e) {}

    const userId = currentUser ? currentUser.id : 'local-user';
    const entries = board.slice(0, limit).map((e, i) => ({
      rank: i + 1,
      playerName: e.playerName,
      displayValue: e.displayValue,
      isCurrentUser: e.userId === userId
    }));

    if (requestId) {
      receiveMessage({
        data: { type: 'mesa:leaderboard:response', requestId, entries }
      });
    }
  }

  // Message Handler
  function receiveMessage(event) {
    const data = event.data;
    if (!data || typeof data !== 'object') return;
    
    // Filter out messages not from Mesa Portal (unless local simulation)
    // In a real implementation, we would check event.origin against an allowlist
    
    if (data.type === 'mesa:init') {
      environment = data.config?.env || 'mesa';
      currentUser = data.user || null;
      currentConfig = data.config || {};
      sessionNonce = data.nonce || null;
      isInitialized = true;
      if (initPromiseResolve) {
        initPromiseResolve();
        initPromiseResolve = null;
      }
      return;
    }

    // Handle error events from portal
    if (data.type === 'mesa:error') {
      emit('error', {
        operation: data.operation,
        code: data.code,
        message: data.message
      });
      return;
    }

    // Handle Responses
    if (data.requestId && pendingRequests.has(data.requestId)) {
      const { resolve, timer } = pendingRequests.get(data.requestId);
      clearTimeout(timer);
      pendingRequests.delete(data.requestId);
      resolve(data);
    }
  }

  window.addEventListener('message', receiveMessage);

  // Promise-based request wrapper
  function sendRequest(type, payload = {}) {
    return new Promise((resolve, reject) => {
      const requestId = generateId();
      
      const timer = setTimeout(() => {
        if (pendingRequests.has(requestId)) {
          pendingRequests.delete(requestId);
          // Don't reject, just resolve with error to avoid crashing games
          const error = { error: { code: ErrorCode.TIMEOUT, message: 'Request timed out' } };
          emit('error', { operation: type, ...error.error });
          console.warn(`Mesa SDK: Request ${type} timed out`);
          resolve(error);
        }
      }, REQUEST_TIMEOUT);

      pendingRequests.set(requestId, { resolve, reject, timer });
      Transport.send(type, { ...payload, requestId });
    });
  }

  // Debounce helper for setItem
  const saveDebounceTimers = new Map();
  function debouncedSave(key, value) {
    return new Promise((resolve, reject) => {
      if (saveDebounceTimers.has(key)) {
        clearTimeout(saveDebounceTimers.get(key).timer);
        const prev = saveDebounceTimers.get(key);
        prev.resolve({ superseded: true }); 
      }

      const timer = setTimeout(async () => {
        saveDebounceTimers.delete(key);
        try {
          const res = await sendRequest('mesa:data:set', { key, value });
          resolve(res);
        } catch (e) {
          resolve({ error: { code: ErrorCode.NETWORK_ERROR, message: e.message } });
        }
      }, 1000); // 1 second debounce

      saveDebounceTimers.set(key, { timer, resolve });
    });
  }

  // Public API
  const Mesa = {
    version: MESA_VERSION,
    ErrorCode: ErrorCode,

    init: function () {
      return new Promise((resolve) => {
        if (isInitialized) {
          resolve();
          return;
        }
        
        initPromiseResolve = resolve;

        // Check if we are standalone
        if (window.parent === window) {
          console.log('Mesa SDK: Running standalone, switching to local mode.');
          environment = 'local';
          // Trigger local init
          handleLocalRequest('mesa:ready', {});
        } else {
          // Send handshake
          Transport.send('mesa:ready');
          
          // Fallback if no response in 1s (maybe portal is down or not a mesa portal)
          setTimeout(() => {
            if (!isInitialized) {
              console.warn('Mesa SDK: No handshake received, falling back to local mode.');
              environment = 'local';
              handleLocalRequest('mesa:ready', {});
            }
          }, 1000);
        }
      });
    },

    getEnvironment: function () {
      return environment;
    },

    // Event system
    on: function (eventName, callback) {
      if (!eventListeners.has(eventName)) {
        eventListeners.set(eventName, []);
      }
      eventListeners.get(eventName).push(callback);
    },

    off: function (eventName, callback) {
      if (!eventListeners.has(eventName)) return;
      const listeners = eventListeners.get(eventName);
      const idx = listeners.indexOf(callback);
      if (idx >= 0) listeners.splice(idx, 1);
    },

    user: {
      get: function () {
        return currentUser;
      },
      isLoggedIn: function () {
        return !!currentUser && currentUser.id !== 'local-user';
      },
      getLanguages: function() {
        // Returns ordered array of preferred language codes (e.g. ['en', 'cs'])
        const locales = getUserLocales();
        const result = [];
        const seen = new Set();
        
        for (const locale of locales) {
          const code = locale.split('_')[0];
          if (!seen.has(code)) {
            result.push(code);
            seen.add(code);
          }
        }
        return result;
      },
      getLocales: function() {
        // Returns ordered array of preferred locale codes (e.g. ['en_us', 'cs_cz'])
        return getUserLocales();
        return { width: 1920, height: 1080, aspectRatio: '16:9' };
      },
      getContainerSize: function () {
        // Return actual iframe dimensions
        return {
          width: window.innerWidth,
          height: window.innerHeight
        };
      }
    },

    data: {
      getItem: async function (key) {
        if (!isInitialized) {
          return { error: { code: ErrorCode.NOT_INITIALIZED, message: 'SDK not initialized' } };
        }
        const res = await sendRequest('mesa:data:get', { key });
        if (res.error) return res;
        return res.value || null;
      },
      setItem: function (key, value) {
        if (!isInitialized) {
          return Promise.resolve({ error: { code: ErrorCode.NOT_INITIALIZED, message: 'SDK not initialized' } });
        }
        // Use debounce
        return debouncedSave(key, value);
      },
      removeItem: async function (key) {
        if (!isInitialized) {
          return { error: { code: ErrorCode.NOT_INITIALIZED, message: 'SDK not initialized' } };
        }
        return sendRequest('mesa:data:remove', { key });
      },
      clear: async function () {
        if (!isInitialized) {
          return { error: { code: ErrorCode.NOT_INITIALIZED, message: 'SDK not initialized' } };
        }
        return sendRequest('mesa:data:clear');
      }
    },

    leaderboard: {
      /**
       * Submit a score to the leaderboard. Creates or updates if better.
       * @param {Object} options
       * @param {string} [options.key='default'] - Leaderboard identifier
       * @param {string} options.playerName - Display name (required)
       * @param {string} options.displayValue - What players see, e.g., "18.5s"
       * @param {number} options.sortValue - Numeric for sorting (higher = better)
       * @returns {Promise<{success: boolean, rank: number, isNewBest: boolean} | {error: Object}>}
       */
      /**
       * Submit a score to the leaderboard. Creates or updates if better.
       * Requires user to be logged in.
       * @param {Object} options
       * @param {string} [options.key='default'] - Leaderboard identifier
       * @param {string} options.playerName - Display name (required)
       * @param {string} options.displayValue - What players see, e.g., "18.5s"
       * @param {number} options.sortValue - Numeric for sorting (higher = better)
       * @returns {Promise<{success: boolean, rank: number|null, isNewBest: boolean, onLeaderboard: boolean} | {error: Object}>}
       */
      submit: async function (options) {
        if (!isInitialized) {
          return { error: { code: ErrorCode.NOT_INITIALIZED, message: 'SDK not initialized' } };
        }

        // Require login for leaderboard submissions
        if (!currentUser || currentUser.id === 'local-user') {
          return { error: { code: ErrorCode.NOT_LOGGED_IN, message: 'You must be logged in to submit scores' } };
        }

        const { key = 'default', playerName, displayValue, sortValue } = options || {};

        // Validate inputs
        if (!playerName || typeof playerName !== 'string' || playerName.length > 50) {
          return { error: { code: ErrorCode.INVALID_INPUT, message: 'playerName is required and must be <= 50 chars' } };
        }
        if (!displayValue || typeof displayValue !== 'string' || displayValue.length > 50) {
          return { error: { code: ErrorCode.INVALID_INPUT, message: 'displayValue is required and must be <= 50 chars' } };
        }
        if (typeof sortValue !== 'number' || isNaN(sortValue)) {
          return { error: { code: ErrorCode.INVALID_INPUT, message: 'sortValue must be a number' } };
        }

        const res = await sendRequest('mesa:leaderboard:submit', { key, playerName, displayValue, sortValue });
        return res;
      },

      /**
       * Get leaderboard entries centered around the current player.
       * If logged in, returns 50 entries centered around player's rank.
       * If player is not on leaderboard (rank > 1000), returns last 50 entries (951-1000).
       * If not logged in, returns top 50 entries.
       * @param {Object} [options]
       * @param {string} [options.key='default'] - Leaderboard identifier
       * @returns {Promise<{playerRank: number|null, entries: Array, totalEntries: number, isBelowThreshold: boolean}>}
       */
      get: async function (options) {
        if (!isInitialized) {
          return { error: { code: ErrorCode.NOT_INITIALIZED, message: 'SDK not initialized' } };
        }

        const { key = 'default' } = options || {};
        const res = await sendRequest('mesa:leaderboard:get', { key });
        return res;
      },

      /**
       * Get top entries from a leaderboard.
       * @param {Object} [options]
       * @param {string} [options.key='default'] - Leaderboard identifier
       * @param {number} [options.limit=10] - Number of entries to fetch
       * @returns {Promise<{entries: Array}>}
       */
      getTop: async function (options) {
        if (!isInitialized) {
          return { error: { code: ErrorCode.NOT_INITIALIZED, message: 'SDK not initialized' } };
        }

        const { key = 'default', limit = 10 } = options || {};
        const res = await sendRequest('mesa:leaderboard:getTop', { key, limit: Math.min(100, Math.max(1, limit)) });
        return res;
      }
    },

    game: {
      gameplayStart: function () {
        Transport.send('mesa:game:event', { event: 'gameplayStart' });
      },
      gameplayStop: function () {
        Transport.send('mesa:game:event', { event: 'gameplayStop' });
      },
      loadingStart: function () {
        Transport.send('mesa:game:event', { event: 'loadingStart' });
      },
      loadingEnd: function () {
        Transport.send('mesa:game:event', { event: 'loadingEnd' });
      }
    },

    log: {
      /**
       * Log an info message to the console with Mesa branding.
       * @param {...*} args - Arguments to log
       */
      info: function (...args) {
        console.log('%c Mesa ', 'background: #3b82f6; color: white; border-radius: 3px; font-weight: bold;', ...args);
      },
      /**
       * Log a warning message to the console with Mesa branding.
       * @param {...*} args - Arguments to log
       */
      warn: function (...args) {
        console.warn('%c Mesa ', 'background: #f59e0b; color: white; border-radius: 3px; font-weight: bold;', ...args);
      },
      /**
       * Log an error message to the console with Mesa branding.
       * @param {...*} args - Arguments to log
       */
      error: function (...args) {
        console.error('%c Mesa ', 'background: #ef4444; color: white; border-radius: 3px; font-weight: bold;', ...args);
      }
    }
  };

  window.Mesa = Mesa;

})(window);
