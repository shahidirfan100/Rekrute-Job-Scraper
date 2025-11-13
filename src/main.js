// Rekrute.com jobs scraper - production-ready CheerioCrawler implementation
// Stack: Apify SDK + Crawlee + CheerioCrawler + header-generator + jsdom

import { Actor, log } from 'apify';
import {
    CheerioCrawler,
    Dataset,
    sleep,
    RequestQueue,
} from 'crawlee';
import { load as cheerioLoad } from 'cheerio';
import { HeaderGenerator } from 'header-generator';
import { JSDOM } from 'jsdom';

// ---------- Global config & helpers ----------

const headerGenerator = new HeaderGenerator({
    browsers: ['chrome'],
    devices: ['desktop'],
    operatingSystems: ['windows', 'linux'],
    locales: ['fr-FR', 'fr', 'en-US', 'en'],
});

function toPositiveInt(value, defaultValue, { min = 1, max = 100000 } = {}) {
    const n = Number(value);
    if (!Number.isFinite(n)) return defaultValue;
    return Math.min(max, Math.max(min, Math.floor(n)));
}

function toAbs(href, base) {
    try {
        return new URL(href, base).href;
    } catch {
        return null;
    }
}

function isBlocked($) {
    const text = $('body').text().toLowerCase();
    return /captcha|access denied|forbidden|blocked|unusual traffic/.test(text);
}

function cleanText(htmlOrText) {
    if (!htmlOrText) return null;
    const $ = cheerioLoad(`<div id="root">${htmlOrText}</div>`);
    let text = $('#root').text();

    if (!text) return null;

    text = text
        .replace(/\r\n|\r/g, '\n')
        .replace(/\u00a0/g, ' ')
        .replace(/\s+\n/g, '\n')
        .replace(/\n\s+/g, '\n')
        .replace(/[ \t]{2,}/g, ' ')
        .trim();

    return text || null;
}

/**
 * JSON-LD JobPosting extraction.
 */
function extractFromJsonLd($) {
    const scripts = $('script[type="application/ld+json"]');

    for (let i = 0; i < scripts.length; i++) {
        try {
            const jsonText = $(scripts[i]).html() || '';
            if (!jsonText.trim()) continue;

            const parsed = JSON.parse(jsonText);
            const nodes = [];
            const addNode = (node) => {
                if (!node) return;
                if (Array.isArray(node)) {
                    node.forEach(addNode);
                } else if (node['@graph']) {
                    addNode(node['@graph']);
                } else {
                    nodes.push(node);
                }
            };
            addNode(parsed);

            for (const e of nodes) {
                const t = e['@type'] || e.type;
                const types = Array.isArray(t) ? t : [t];
                if (!types.includes('JobPosting')) continue;

                const jobLoc = e.jobLocation || {};
                const addr = jobLoc.address || {};

                return {
                    raw: e,
                    title: e.title || e.name || null,
                    company: e.hiringOrganization?.name || null,
                    datePosted: e.datePosted || null,
                    descriptionHtml: e.description || null,
                    employmentType: e.employmentType || null,
                    validThrough: e.validThrough || null,
                    location:
                        addr.addressLocality ||
                        addr.addressRegion ||
                        addr.addressCountry ||
                        null,
                    salary: e.baseSalary?.value?.value
                        ? `${e.baseSalary.value.value} ${e.baseSalary.value.currency || ''}`.trim()
                        : null,
                };
            }
        } catch {
            // ignore JSON errors
        }
    }

    return null;
}

/**
 * Parse title + company + possible location from headings.
 */
function parseTitleCompanyLocation($) {
    const h1 = $('h1').first().text().trim();
    let title = null;
    let company = null;
    let location = null;

    if (h1) {
        const parts = h1.split(/\s[-–|]\s/);
        if (parts.length >= 1) title = parts[0]?.trim() || null;
        if (parts.length >= 2) company = parts[1]?.trim() || null;
        if (parts.length >= 3) location = parts[2]?.trim() || null;
    }

    if (!company) {
        const companySel =
            '.company, .company-name, .societe, .society, a.company, a.company-name';
        company = $(companySel).first().text().trim() || null;
    }

    if (!location) {
        const microLoc = $('[itemprop="jobLocation"] [itemprop="addressLocality"], [itemprop="addressLocality"]').first().text().trim();
        if (microLoc) location = microLoc;
    }

    if (!location) {
        const locLabel = $('p:contains("Poste basé à"), p:contains("Localisation"), p:contains("Location")')
            .first()
            .text();
        if (locLabel) {
            const m = locLabel.match(/(?:basé à|Localisation|Location)\s*[:\-]?\s*(.*)/i);
            if (m && m[1]) location = m[1].trim();
        }
    }

    return { title, company, location };
}

/**
 * First sanitizer: remove scripts/styles/forms/nav/sidebar/footer etc.
 * Then pick densest block.
 */
function sanitizeDescriptionHtml(html) {
    if (!html) return null;

    const $ = cheerioLoad(`<div id="root">${html}</div>`);

    // Remove noise
    $('#root script, #root style, #root link, #root noscript, #root form').remove();
    $('#root header, #root nav, #root footer').remove();

    const blacklistSelectors = [
        '#fortopscroll',
        '.wrapper',
        '.preloader',
        '#sidebar',
        '#rk-articles',
        '#rk-filter-panel',
        '.pagination',
        '.subbar',
        '.footer',
        'footer',
        '#rk-auth-scope',
        '.modal',
        '.navbar-burger',
    ];
    for (const sel of blacklistSelectors) {
        $(sel).remove();
    }

    // Flatten single-child wrappers
    let changed = true;
    while (changed) {
        changed = false;
        $('#root > div, #root > section, #root > article').each((_, el) => {
            const $el = $(el);
            const children = $el.children('div, section, article');
            const textLen = $el.text().replace(/\s+/g, ' ').trim().length;
            if (children.length === 1 && textLen === children.text().replace(/\s+/g, ' ').trim().length) {
                $('#root').append(children);
                $el.remove();
                changed = true;
            }
        });
    }

    // densest block in root
    let best = null;
    let bestLen = 0;

    $('#root')
        .find('article, section, div, main')
        .each((_, el) => {
            const $el = $(el);
            const classId = (($el.attr('class') || '') + ' ' + ($el.attr('id') || '')).toLowerCase();
            if (/footer|nav|menu|breadcrumb|sidebar|filter|pagination/.test(classId)) return;

            const text = $el.text().replace(/\s+/g, ' ').trim();
            const len = text.length;
            if (len > bestLen) {
                bestLen = len;
                best = el;
            }
        });

    let finalHtml;
    if (best && bestLen > 80) {
        const $wrap = cheerioLoad('<div></div>');
        $wrap('div').append(cheerioLoad(best).html() || '');
        finalHtml = $wrap('div').html();
    } else {
        finalHtml = $('#root').html();
    }

    finalHtml = (finalHtml || '').trim();
    return finalHtml || null;
}

/**
 * FINAL step for description_html:
 *  - keep only "text-related" tags
 *  - drop layout tags & attributes (only keep href on <a>).
 */
function simplifyHtmlContent(html) {
    if (!html) return null;

    const allowedTags = new Set([
        'p', 'br',
        'ul', 'ol', 'li',
        'strong', 'b', 'em', 'i', 'u',
        'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
        'a',
    ]);

    const $ = cheerioLoad(`<div id="root">${html}</div>`);

    // unwrap all disallowed tags (div, section, span, etc.)
    $('#root *').each((_, el) => {
        const $el = $(el);
        const tag = el.tagName ? el.tagName.toLowerCase() : '';

        if (!allowedTags.has(tag)) {
            const text = $el.text().trim();
            const hasPChild = $el.find('p').length > 0;

            if (text && !hasPChild && (tag === 'div' || tag === 'section' || tag === 'article')) {
                const newHtml = `<p>${text}</p>`;
                $el.replaceWith(newHtml);
            } else {
                $el.replaceWith($el.html() || '');
            }
        }
    });

    // Clean attributes & remove empty paragraphs/list items
    $('#root *').each((_, el) => {
        const $el = $(el);
        const tag = el.tagName ? el.tagName.toLowerCase() : '';
        const attrs = el.attribs || {};

        Object.keys(attrs).forEach((attr) => {
            $el.removeAttr(attr);
        });

        if (tag === 'a' && attrs.href) {
            $el.attr('href', attrs.href);
        }

        if ((tag === 'p' || tag === 'li') && $el.text().replace(/\u00a0/g, ' ').trim().length === 0) {
            $el.remove();
        }
    });

    const result = $('#root').html() || '';
    const trimmed = result.trim();
    return trimmed || null;
}

/**
 * Job description extraction (before simplification).
 */
function getDescriptionHtml($) {
    let html = '';

    const frPoste = $('h2:contains("Poste :"), h2:contains("Poste")')
        .first()
        .nextUntil('h2')
        .html();
    const frProfil = $('h2:contains("Profil recherché :"), h2:contains("Profil recherché")')
        .first()
        .nextUntil('h2')
        .html();
    const enPoste = $('h2:contains("Job Description"), h2:contains("Position"), h2:contains("Role")')
        .first()
        .nextUntil('h2')
        .html();
    const enProfil = $('h2:contains("Profile"), h2:contains("Requirements"), h2:contains("Responsibilities")')
        .first()
        .nextUntil('h2')
        .html();

    if (frPoste) html += frPoste;
    if (frProfil) html += frProfil;
    if (enPoste) html += enPoste;
    if (enProfil) html += enProfil;

    // detail page containers
    if (!html) {
        const jobSelectors = [
            '#job_desc',
            '#job-detail',
            '.job-description',
            '.job-desc',
            '.job-detail',
            '.jobdetail',
            '.job-content',
            '.job-body',
            '.jobbody',
            '.jobdescription',
            '[itemprop="description"]',
        ];

        for (const sel of jobSelectors) {
            const el = $(sel).first();
            if (!el.length) continue;
            const textLen = el.text().replace(/\s+/g, ' ').trim().length;
            if (textLen > 80) {
                html = el.html() || '';
                break;
            }
        }
    }

    // fallback: central column
    if (!html) {
        const centerSelectors = [
            '.content-column',
            '.col-md-9',
            '#job',
            '#job-info',
        ];
        let root = null;
        for (const sel of centerSelectors) {
            const el = $(sel).first();
            if (el.length) {
                root = el;
                break;
            }
        }
        if (root) {
            let bestHtml = null;
            let bestLen = 0;
            root.find('div, section, article').each((_, el) => {
                const $el = $(el);
                const classId = (($el.attr('class') || '') + ' ' + ($el.attr('id') || '')).toLowerCase();
                if (/sidebar|filter|pagination|breadcrumb|footer/.test(classId)) return;
                const textLen = $el.text().replace(/\s+/g, ' ').trim().length;
                if (textLen > bestLen) {
                    bestLen = textLen;
                    bestHtml = $el.html() || '';
                }
            });
            if (bestHtml && bestLen > 80) html = bestHtml;
        }
    }

    if (!html) return null;

    try {
        const dom = new JSDOM(`<div id="root">${html}</div>`);
        const root = dom.window.document.getElementById('root');
        const raw = root.innerHTML || html;
        return sanitizeDescriptionHtml(raw);
    } catch {
        return sanitizeDescriptionHtml(html);
    }
}

/**
 * Date posted.
 */
function getDatePosted($) {
    const frText = $('p:contains("Publiée"), span:contains("Publiée")').first().text();
    if (frText) {
        const m = frText.match(/Publiée\s+(?:le\s+)?(.*)/i);
        if (m && m[1]) return m[1].trim();
    }

    const enText = $('p:contains("Published"), p:contains("Posted"), span:contains("Published"), span:contains("Posted")')
        .first()
        .text();
    if (enText) {
        const m = enText.match(/(?:Published|Posted)\s+(?:on\s+)?(.*)/i);
        if (m && m[1]) return m[1].trim();
    }

    const bodyText = $('body').text();
    const dateMatch = bodyText.match(/(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})/);
    if (dateMatch && dateMatch[1]) {
        return dateMatch[1];
    }

    return null;
}

function getEmploymentType($, jsonLd, descriptionText) {
    if (jsonLd?.employmentType) return jsonLd.employmentType;

    const text = (descriptionText || $('body').text() || '').toLowerCase();

    if (/cdi\b/.test(text)) return 'CDI';
    if (/cdd\b/.test(text)) return 'CDD';
    if (/full[-\s]?time|temps plein/.test(text)) return 'FULL_TIME';
    if (/part[-\s]?time|temps partiel/.test(text)) return 'PART_TIME';
    if (/stage|internship/.test(text)) return 'INTERNSHIP';
    if (/freelance|indépendant|independent contractor/.test(text)) return 'CONTRACTOR';

    return null;
}

function getSalary($, jsonLd, descriptionText) {
    if (jsonLd?.salary) return jsonLd.salary;

    const text = (descriptionText || $('body').text() || '');

    const regexes = [
        /(\d[\d\s\.]{2,})\s*(MAD|DH|DHS|€|EUR)/i,
        /(salaire|rémunération)\s*[:\-]?\s*([^\n]+)/i,
    ];

    for (const re of regexes) {
        const m = text.match(re);
        if (m) {
            if (m[1] && m[2]) return `${m[1].trim()} ${m[2].trim()}`;
            if (m[2]) return m[2].trim();
            if (m[1]) return m[1].trim();
        }
    }

    return null;
}

/**
 * Infer language.
 */
function detectLanguage($, url) {
    const htmlLang =
        $('html').attr('lang') ||
        $('html').attr('xml:lang') ||
        $('meta[http-equiv="content-language"]').attr('content') ||
        '';

    const langLower = htmlLang.toLowerCase();

    if (langLower.startsWith('fr')) return 'fr';
    if (langLower.startsWith('en')) return 'en';

    if (/\/en\//i.test(url)) return 'en';
    if (/rekrute\.com\/(fr|offres)/i.test(url)) return 'fr';

    const bodyText = $('body').text().toLowerCase();
    const frHits = (bodyText.match(/\boffre d'emploi|poste|profil recherché|contrat|mission\b/g) || []).length;
    const enHits = (bodyText.match(/\bjob|position|requirements|responsibilities|full-time|part-time\b/g) || []).length;

    if (frHits > enHits) return 'fr';
    if (enHits > frHits) return 'en';

    return null;
}

/**
 * STRICT job-detail URL recognition + structural selectors.
 * This is where Afrique listing previously got misclassified as a job.
 */
function findJobLinks($, baseUrl) {
    const links = new Set();

    // 1) Structural selectors (the standard Rekrute cards)
    $('ul.job-list li.post-id a.titreJob, ul.job-list2 li.post-id a.titreJob, a.titreJob').each((_, el) => {
        const href = $(el).attr('href');
        if (!href) return;
        const abs = toAbs(href, baseUrl);
        if (!abs) return;

        try {
            const url = new URL(abs);
            if (!/\.?rekrute\.com$/i.test(url.hostname)) return;

            const path = url.pathname || '';

            // Ignore clear listing paths (offres = plural / list)
            if (/\/offres/i.test(path) || /offres\.html$/i.test(path)) return;

            // Require a detail slug with numeric ID at the end, e.g.:
            // /offre-emploi-some-title-177064.html
            if (/\/offre-emploi[^/]*-\d+\.html$/i.test(path) || /\/job-offer[^/]*-\d+\.html$/i.test(path)) {
                links.add(url.href);
            }
        } catch {
            // ignore malformed URLs
        }
    });

    // 2) Generic fallback over all anchors (extra safety)
    if (links.size === 0) {
        $('a[href]').each((_, el) => {
            const href = $(el).attr('href');
            if (!href) return;
            const abs = toAbs(href, baseUrl);
            if (!abs) return;

            try {
                const url = new URL(abs);
                if (!/\.?rekrute\.com$/i.test(url.hostname)) return;

                const path = url.pathname || '';

                // Skip listing pages
                if (/\/offres/i.test(path) || /offres\.html$/i.test(path)) return;

                if (/\/offre-emploi[^/]*-\d+\.html$/i.test(path) || /\/job-offer[^/]*-\d+\.html$/i.test(path)) {
                    links.add(url.href);
                }
            } catch {
                // ignore
            }
        });
    }

    return [...links];
}

/**
 * Next page from pagination.
 */
function findNextPage($, baseUrl) {
    const relNextHref =
        $('.pagination a[rel="next"]').attr('href') ||
        $('a[rel="next"]').attr('href');

    if (relNextHref) {
        const abs = toAbs(relNextHref, baseUrl);
        if (abs) return abs;
    }

    const nextLink = $('a')
        .filter((_, el) => {
            const txt = $(el).text().trim().toLowerCase();
            return /(suivant|next|›|»|>)/.test(txt);
        })
        .first()
        .attr('href');

    if (nextLink) {
        const abs = toAbs(nextLink, baseUrl);
        if (abs) return abs;
    }

    return null;
}

function buildStartUrl({ keyword, location, category, lang }) {
    let base;
    if (lang === 'en') {
        base = 'https://www.rekrute.com/en/offres.html';
    } else {
        base = 'https://www.rekrute.com/offres.html';
    }

    const u = new URL(base);
    u.searchParams.set('clear', '1');

    if (keyword) u.searchParams.set('keyword', String(keyword).trim());
    if (location) u.searchParams.set('jobLocation', String(location).trim());
    if (category) u.searchParams.set('category', String(category).trim());

    return u.href;
}

/**
 * Fallback for location from header / URL.
 */
function inferLocationFallback($, url, existingLocation) {
    if (existingLocation) return existingLocation;

    const headerText =
        $('.page-heading h1').first().text() ||
        $('h1').first().text() ||
        $('title').first().text() ||
        '';

    if (headerText) {
        const m = headerText.match(/\|\s*([^|]+?\(Morocco\)|[^|]+?\(Maroc\))/i);
        if (m && m[1]) return m[1].trim();
    }

    const teaser = $('a.titreJob').first().text();
    if (teaser) {
        const m = teaser.match(/\|\s*([^|]+?\(Morocco\)|[^|]+?\(Maroc\))/i);
        if (m && m[1]) return m[1].trim();
    }

    try {
        const path = new URL(url).pathname;
        const slugMatch = path.match(/-([a-zA-ZÀ-ÿ]+)-\d+\.html$/i);
        if (slugMatch && slugMatch[1]) {
            const citySlug = slugMatch[1];
            const city = citySlug
                .split('-')
                .map((p) => p.charAt(0).toUpperCase() + p.slice(1).toLowerCase())
                .join('-');
            return city;
        }
    } catch {
        // ignore
    }

    return existingLocation || null;
}

// ---------- Main actor ----------

await Actor.init();

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            keyword = '',
            location = '',
            category = '',
            results_wanted: RESULTS_WANTED_RAW = 100,
            max_pages: MAX_PAGES_RAW = 999,
            collectDetails = true,
            startUrl,
            startUrls,
            url,
            proxyConfiguration,
            lang = 'fr', // 'fr' | 'en' | 'both'
            maxConcurrency: MAX_CONCURRENCY_RAW = 20,
        } = input;

        const RESULTS_WANTED = toPositiveInt(RESULTS_WANTED_RAW, 100, { min: 1, max: 10000 });
        const MAX_PAGES = toPositiveInt(MAX_PAGES_RAW, 999, { min: 1, max: 10000 });
        const MAX_CONCURRENCY = toPositiveInt(MAX_CONCURRENCY_RAW, 20, { min: 1, max: 100 });

        log.info('Input received', {
            keyword,
            location,
            category,
            RESULTS_WANTED,
            MAX_PAGES,
            collectDetails,
            lang,
            MAX_CONCURRENCY,
        });

        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) {
            for (const u of startUrls) {
                if (u) initial.push(String(u));
            }
        }
        if (startUrl) initial.push(String(startUrl));
        if (url) initial.push(String(url));

        if (!initial.length) {
            if (lang === 'both') {
                initial.push(buildStartUrl({ keyword, location, category, lang: 'fr' }));
                initial.push(buildStartUrl({ keyword, location, category, lang: 'en' }));
            } else {
                initial.push(buildStartUrl({ keyword, location, category, lang }));
            }
        }

        log.info('Initial start URLs:', initial);

        const proxyConf = proxyConfiguration
            ? await Actor.createProxyConfiguration(proxyConfiguration)
            : null;

        const requestQueue = await RequestQueue.open();

        for (const start of initial) {
            await requestQueue.addRequest({
                url: start,
                userData: {
                    label: 'LIST',
                    pageNo: 1,
                },
            });
        }

        const visitedListUrls = new Set();
        const seenJobUrls = new Set();

        let saved = 0;
        let queuedDetail = 0;
        let listPages = 0;
        let detailPages = 0;

        const crawler = new CheerioCrawler({
            requestQueue,
            proxyConfiguration: proxyConf || undefined,
            maxConcurrency: MAX_CONCURRENCY,
            requestHandlerTimeoutSecs: 120,
            navigationTimeoutSecs: 60,
            maxRequestRetries: 3,
            useSessionPool: true,
            sessionPoolOptions: {
                maxPoolSize: Math.max(50, MAX_CONCURRENCY * 2),
                sessionOptions: { maxUsageCount: 50 },
            },
            preNavigationHooks: [
                async (crawlingContext) => {
                    const { request, session } = crawlingContext;

                    const generated = headerGenerator.getHeaders({
                        httpVersion: '2',
                    });

                    request.headers = {
                        ...generated,
                        ...request.headers,
                    };

                    await sleep(500 + Math.random() * 1500);

                    log.debug('Requesting URL', {
                        url: request.url,
                        sessionId: session?.id,
                    });
                },
            ],
            failedRequestHandler: async ({ request, error, session }) => {
                log.error(`Request failed ${request.url}: ${error?.message || error}`);
                if (session) session.retire();
            },
            requestHandler: async (ctx) => {
                const { request, $, body, session } = ctx;
                const label = request.userData.label || 'LIST';
                const pageNo = request.userData.pageNo || 1;

                let $page = $;
                if (!$page) {
                    const html = typeof body === 'string' ? body : body?.toString('utf8');
                    if (!html) {
                        log.warning(`Empty body for ${label} ${request.url}`);
                        return;
                    }
                    $page = cheerioLoad(html);
                }

                if (isBlocked($page)) {
                    log.warning(`Potentially blocked page at ${request.url}`);
                    if (session) session.retire();
                    return;
                }

                if (label === 'LIST') {
                    listPages++;
                    const listUrl = request.loadedUrl || request.url;

                    if (visitedListUrls.has(listUrl)) {
                        log.debug(`Already visited LIST URL, skipping: ${listUrl}`);
                        return;
                    }
                    visitedListUrls.add(listUrl);

                    const remaining = RESULTS_WANTED - saved - queuedDetail;
                    if (remaining <= 0) {
                        log.info(`Reached RESULTS_WANTED, skipping new links on LIST.`);
                        return;
                    }

                    const jobLinks = findJobLinks($page, listUrl);
                    log.info(`LIST page ${pageNo} (${listUrl}) job links found: ${jobLinks.length}`);

                    if (!jobLinks.length) {
                        const snippet = $page('body').text().trim().slice(0, 200);
                        log.warning(
                            `No job links found on LIST page ${pageNo}: ${listUrl}. Body snippet: "${snippet}..."`
                        );
                    }

                    const newJobLinks = [];
                    for (const link of jobLinks) {
                        if (seenJobUrls.has(link)) continue;
                        seenJobUrls.add(link);
                        newJobLinks.push(link);
                    }

                    if (collectDetails) {
                        const allowed = newJobLinks.slice(0, remaining);
                        for (const jobUrl of allowed) {
                            await requestQueue.addRequest({
                                url: jobUrl,
                                userData: { label: 'DETAIL' },
                            });
                            queuedDetail++;
                        }
                        log.info(
                            `Queued ${allowed.length} DETAIL requests (queuedDetail=${queuedDetail}, saved=${saved}).`
                        );
                    } else {
                        const allowed = newJobLinks.slice(0, remaining);
                        for (const jobUrl of allowed) {
                            await Dataset.pushData({
                                url: jobUrl,
                                source: 'rekrute.com',
                                discoveredOn: listUrl,
                                pageNo,
                                scrapedAt: new Date().toISOString(),
                            });
                            saved++;
                        }
                        log.info(`Saved ${allowed.length} URL-only items (saved=${saved}/${RESULTS_WANTED}).`);
                    }

                    if (pageNo < MAX_PAGES && saved < RESULTS_WANTED) {
                        const nextPageUrl = findNextPage($page, listUrl);
                        if (nextPageUrl && !visitedListUrls.has(nextPageUrl)) {
                            log.info(`Enqueuing next LIST page: ${nextPageUrl}`);
                            await requestQueue.addRequest({
                                url: nextPageUrl,
                                userData: { label: 'LIST', pageNo: pageNo + 1 },
                            });
                        } else if (nextPageUrl) {
                            log.debug(`Next LIST page already seen/queued: ${nextPageUrl}`);
                        } else {
                            log.info(`No next LIST page found from ${listUrl}`);
                        }
                    } else if (pageNo >= MAX_PAGES) {
                        log.info(`Reached MAX_PAGES=${MAX_PAGES}, stopping pagination.`);
                    }
                } else if (label === 'DETAIL') {
                    detailPages++;

                    if (!collectDetails) {
                        log.debug(`DETAIL reached but collectDetails=false, skipping: ${request.url}`);
                        return;
                    }

                    if (saved >= RESULTS_WANTED) {
                        log.info(
                            `Saved >= RESULTS_WANTED (${RESULTS_WANTED}), skipping DETAIL: ${request.url}`
                        );
                        return;
                    }

                    const detailUrl = request.loadedUrl || request.url;

                    try {
                        const pageLang = detectLanguage($page, detailUrl);

                        const jsonLd = extractFromJsonLd($page);
                        const { title: titleHtml, company: companyHtml, location: locationHtml } =
                            parseTitleCompanyLocation($page);

                        const descriptionHtmlDom = getDescriptionHtml($page);

                        const candidateHtml =
                            jsonLd?.descriptionHtml ||
                            jsonLd?.raw?.description ||
                            descriptionHtmlDom ||
                            null;

                        let descriptionHtml = candidateHtml
                            ? sanitizeDescriptionHtml(candidateHtml)
                            : null;

                        // Simplify to text-related tags only
                        descriptionHtml = simplifyHtmlContent(descriptionHtml);

                        const descriptionText = cleanText(descriptionHtml);

                        const datePostedLd = jsonLd?.datePosted || null;
                        const datePostedText = getDatePosted($page);
                        const datePosted = datePostedLd || datePostedText || null;

                        const finalTitle = jsonLd?.title || titleHtml || null;
                        const finalCompany = jsonLd?.company || companyHtml || null;

                        let finalLocation =
                            jsonLd?.location ||
                            locationHtml ||
                            null;
                        finalLocation = inferLocationFallback($page, detailUrl, finalLocation);

                        const employmentType = getEmploymentType($page, jsonLd, descriptionText);
                        const salary = getSalary($page, jsonLd, descriptionText);

                        const record = {
                            url: detailUrl,
                            title: finalTitle,
                            company: finalCompany,
                            location: finalLocation || null,
                            datePosted,
                            employmentType: employmentType || null,
                            validThrough: jsonLd?.validThrough || null,
                            salary: salary || null,
                            descriptionHtml: descriptionHtml || null,
                            descriptionText: descriptionText || null,
                            language: pageLang || null,
                            source: 'rekrute.com',
                            scrapedAt: new Date().toISOString(),
                        };

                        await Dataset.pushData(record);
                        saved++;

                        log.info(`Saved DETAIL ${saved}/${RESULTS_WANTED}: ${detailUrl}`);
                    } catch (err) {
                        log.error(`Failed to process DETAIL ${request.url}: ${err.message}`);
                        if (session) session.retire();
                    }
                } else {
                    log.warning(`Unknown label "${label}" for URL ${request.url}, skipping.`);
                }
            },
        });

        await crawler.run();

        log.info('Crawl finished', {
            saved,
            RESULTS_WANTED,
            listPages,
            detailPages,
            queuedDetail,
        });
    } finally {
        await Actor.exit();
    }
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
