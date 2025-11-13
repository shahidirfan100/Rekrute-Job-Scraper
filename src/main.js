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

// Reuse a single header generator for stealthy browser-like headers.
const headerGenerator = new HeaderGenerator({
    browsers: ['chrome'],
    devices: ['desktop'],
    operatingSystems: ['windows', 'linux'],
    locales: ['fr-FR', 'fr', 'en-US', 'en'],
});

/**
 * Normalize and clamp numeric input.
 */
function toPositiveInt(value, defaultValue, { min = 1, max = 100000 } = {}) {
    const n = Number(value);
    if (!Number.isFinite(n)) return defaultValue;
    return Math.min(max, Math.max(min, Math.floor(n)));
}

/**
 * Return absolute URL given a possibly relative href and base.
 */
function toAbs(href, base) {
    try {
        return new URL(href, base).href;
    } catch {
        return null;
    }
}

/**
 * Basic "blocked/captcha" heuristic.
 */
function isBlocked($) {
    const text = $('body').text().toLowerCase();
    return /captcha|access denied|forbidden|blocked|unusual traffic/.test(text);
}

/**
 * Clean HTML to a normalized text string.
 */
function cleanText(htmlOrText) {
    if (!htmlOrText) return null;

    // If it's HTML, strip tags via cheerio
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
 * Try to extract JobPosting data from JSON-LD.
 * Returns a plain object with a few normalized fields or null.
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

                if (types.includes('JobPosting')) {
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
            }
        } catch {
            // ignore JSON errors and continue
        }
    }

    return null;
}

/**
 * Parse title, company, location from heading + DOM.
 * Multi-language friendly and defensive.
 */
function parseTitleCompanyLocation($) {
    // Primary: <h1>Title - Company - Location</h1> (or variations)
    const h1 = $('h1').first().text().trim();
    let title = null;
    let company = null;
    let location = null;

    if (h1) {
        const parts = h1.split(/\s[-–|]\s/); // split on -, – or |
        if (parts.length >= 1) title = parts[0]?.trim() || null;
        if (parts.length >= 2) company = parts[1]?.trim() || null;
        if (parts.length >= 3) location = parts[2]?.trim() || null;
    }

    // Fallback: dedicated company element
    if (!company) {
        const companySel =
            '.company, .company-name, .societe, .society, a.company, a.company-name';
        company = $(companySel).first().text().trim() || null;
    }

    // Microdata / schema.org addressLocality
    if (!location) {
        const microLoc = $('[itemprop="jobLocation"] [itemprop="addressLocality"], [itemprop="addressLocality"]').first().text().trim();
        if (microLoc) location = microLoc;
    }

    // Fallback: location label in FR/EN
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
 * Pick main content container by heuristics (for description fallback).
 */
function findMainContentHtml($) {
    const selectors = [
        '#job_desc', '#job', '#job-info', '#jobInfo', '#jobDetail',
        '.job-description', '.job__description', '.annonce', '.job', '.jobDetail',
        'article.job', 'article'
    ];

    for (const sel of selectors) {
        const el = $(sel).first();
        if (el.length) {
            const textLen = el.text().replace(/\s+/g, ' ').trim().length;
            if (textLen > 200) {
                return el.html() || null;
            }
        }
    }

    // Fallback: densest <div>/<section> in the page body
    let bestHtml = null;
    let bestLen = 0;

    $('div, section').each((_, el) => {
        const $el = $(el);
        // Skip navigation/footer/header-ish elements by class/id
        const id = ($el.attr('id') || '').toLowerCase();
        const cls = ($el.attr('class') || '').toLowerCase();
        if (/header|footer|nav|menu|breadcrumb|sidebar/.test(id + ' ' + cls)) return;

        const textLen = $el.text().replace(/\s+/g, ' ').trim().length;
        if (textLen > bestLen) {
            bestLen = textLen;
            bestHtml = $el.html() || null;
        }
    });

    return bestLen > 100 ? bestHtml : null;
}

/**
 * Extract description HTML from FR and EN section headings,
 * with a broad fallback to main job description container.
 */
function getDescriptionHtml($) {
    let html = '';

    // FR sections
    const posteFr = $('h2:contains("Poste :"), h2:contains("Poste")').nextUntil('h2').html();
    const profilFr = $('h2:contains("Profil recherché :"), h2:contains("Profil recherché")').nextUntil('h2').html();

    // EN sections (approximate)
    const posteEn = $('h2:contains("Job Description"), h2:contains("Position"), h2:contains("Role")')
        .nextUntil('h2')
        .html();
    const profilEn = $('h2:contains("Profile"), h2:contains("Requirements"), h2:contains("Candidate profile")')
        .nextUntil('h2')
        .html();

    if (posteFr) html += posteFr;
    if (profilFr) html += profilFr;
    if (posteEn) html += posteEn;
    if (profilEn) html += profilEn;

    // Broad fallback: main job container (guessing common patterns)
    if (!html) {
        html = findMainContentHtml($) || '';
    }

    if (!html) return null;

    // Optional: run through jsdom for mild normalization
    try {
        const dom = new JSDOM(`<div id="root">${html}</div>`);
        const root = dom.window.document.getElementById('root');
        return root.innerHTML || html;
    } catch {
        return html;
    }
}

/**
 * Extract posted date from textual hints in FR + EN, plus generic date regex.
 */
function getDatePosted($) {
    // FR: "Publiée le ..."
    const frText = $('p:contains("Publiée"), span:contains("Publiée")').first().text();
    if (frText) {
        const m = frText.match(/Publiée\s+(?:le\s+)?(.*)/i);
        if (m && m[1]) return m[1].trim();
    }

    // EN: "Published on ...", "Posted on ..."
    const enText = $('p:contains("Published"), p:contains("Posted"), span:contains("Published"), span:contains("Posted")')
        .first()
        .text();
    if (enText) {
        const m = enText.match(/(?:Published|Posted)\s+(?:on\s+)?(.*)/i);
        if (m && m[1]) return m[1].trim();
    }

    // Generic date pattern somewhere in body (dd/mm/yyyy or dd-mm-yyyy etc.)
    const bodyText = $('body').text();
    const dateMatch = bodyText.match(/(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})/);
    if (dateMatch && dateMatch[1]) {
        return dateMatch[1];
    }

    return null;
}

/**
 * Guess employment type from JSON-LD or body text.
 */
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

/**
 * Guess salary range from JSON-LD or body text.
 */
function getSalary($, jsonLd, descriptionText) {
    if (jsonLd?.salary) return jsonLd.salary;

    const text = (descriptionText || $('body').text() || '');

    // Common currency markers: MAD, DH, DHS, €, EUR
    const regexes = [
        /(\d[\d\s\.]{2,})\s*(MAD|DH|DHS|€|EUR)/i,
        /(salaire|rémunération)\s*[:\-]?\s*([^\n]+)/i,
    ];

    for (const re of regexes) {
        const m = text.match(re);
        if (m) {
            // Prefer numeric + currency if present
            if (m[1] && m[2]) return `${m[1].trim()} ${m[2].trim()}`;
            if (m[2]) return m[2].trim();
            if (m[1]) return m[1].trim();
        }
    }

    return null;
}

/**
 * Infer language from DOM + URL.
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

    // Fallback: naive word-based detection
    const bodyText = $('body').text().toLowerCase();
    const frHits = (bodyText.match(/\boffre d'emploi|poste|profil recherché|contrat|mission\b/g) || []).length;
    const enHits = (bodyText.match(/\bjob|position|requirements|responsibilities|full-time|part-time\b/g) || []).length;

    if (frHits > enHits) return 'fr';
    if (enHits > frHits) return 'en';

    return null;
}

/**
 * Find job detail links on a listing page (FR + EN).
 * Uses domain + path heuristics, then refined regex.
 */
function findJobLinks($, baseUrl) {
    const links = new Set();

    $('a[href]').each((_, el) => {
        const href = $(el).attr('href');
        if (!href) return;

        const abs = toAbs(href, baseUrl);
        if (!abs) return;

        let url;
        try {
            url = new URL(abs);
        } catch {
            return;
        }

        // Host guard: must stay on rekrute.com
        if (!/\.?rekrute\.com$/i.test(url.hostname)) return;

        const path = url.pathname || '';

        // Heuristics for job offer URLs:
        if (
            /\/offre-emploi/i.test(path) ||
            /\/job-offer/i.test(path) ||
            /-emploi-/i.test(path)
        ) {
            links.add(url.href);
        }
    });

    return [...links];
}

/**
 * Find next page URL from pagination.
 * Uses rel="next" and FR+EN "Next" labels.
 */
function findNextPage($, baseUrl) {
    // 1) rel="next"
    const relNextHref =
        $('.pagination a[rel="next"]').attr('href') ||
        $('a[rel="next"]').attr('href');

    if (relNextHref) {
        const abs = toAbs(relNextHref, baseUrl);
        if (abs) return abs;
    }

    // 2) Text-based FR + EN
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

/**
 * Build listing URL based on keyword/location/category and language.
 * Uses `clear=1` as requested.
 */
function buildStartUrl({ keyword, location, category, lang }) {
    let base;
    if (lang === 'en') {
        base = 'https://www.rekrute.com/en/offres.html';
    } else {
        // default FR
        base = 'https://www.rekrute.com/offres.html';
    }

    const u = new URL(base);
    u.searchParams.set('clear', '1');

    if (keyword) u.searchParams.set('keyword', String(keyword).trim());
    if (location) u.searchParams.set('location', String(location).trim());
    if (category) u.searchParams.set('category', String(category).trim());

    return u.href;
}

// ---------- Main actor ----------

await Actor.init(); // for environments that don't use Actor.main

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
            maxConcurrency: MAX_CONCURRENCY_RAW = 20, // << configurable concurrency
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

        // Build initial URLs
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

        log.info(`Initial start URLs:`, initial);

        const proxyConf = proxyConfiguration
            ? await Actor.createProxyConfiguration(proxyConfiguration)
            : null;

        const requestQueue = await RequestQueue.open();

        // Seed queue
        for (const start of initial) {
            await requestQueue.addRequest({
                url: start,
                userData: {
                    label: 'LIST',
                    pageNo: 1,
                },
            });
        }

        // Global state across handlers
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
                sessionOptions: {
                    maxUsageCount: 50,
                },
            },
            preNavigationHooks: [
                async (crawlingContext) => {
                    const { request, session } = crawlingContext;

                    // Generate browser-like headers
                    const generated = headerGenerator.getHeaders({
                        httpVersion: '2',
                    });

                    request.headers = {
                        ...generated,
                        ...request.headers,
                    };

                    // Light randomized delay to look more human
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
                const {
                    request,
                    $,
                    body,
                    contentType,
                    session,
                } = ctx;

                const label = request.userData.label || 'LIST';
                const pageNo = request.userData.pageNo || 1;

                // Ensure we have a cheerio instance
                let $page = $;
                if (!$page) {
                    const html = typeof body === 'string' ? body : body?.toString('utf8');
                    if (!html) {
                        log.warning(`Empty body for ${label} ${request.url}`);
                        return;
                    }
                    $page = cheerioLoad(html);
                }

                // Detect block / captcha
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

                    const anchorCount = $page('a[href]').length;
                    log.info(`LIST page ${pageNo} (${listUrl}) has ${anchorCount} links.`);

                    const remaining = RESULTS_WANTED - saved - queuedDetail;
                    if (remaining <= 0) {
                        log.info(`Already queued/saved ${RESULTS_WANTED} jobs, skipping new links on LIST.`);
                        return;
                    }

                    // Find job links
                    const jobLinks = findJobLinks($page, listUrl);
                    log.info(`Found ${jobLinks.length} job links on LIST page ${pageNo}.`);

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
                            `Queued ${allowed.length} new DETAIL requests (queuedDetail=${queuedDetail}, saved=${saved}).`
                        );
                    } else {
                        // URL-only mode: store URLs directly from list pages
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

                        log.info(
                            `Saved ${allowed.length} URL-only items (saved=${saved}/${RESULTS_WANTED}).`
                        );
                    }

                    // Pagination
                    if (pageNo < MAX_PAGES && saved < RESULTS_WANTED) {
                        const nextPageUrl = findNextPage($page, listUrl);

                        if (nextPageUrl && !visitedListUrls.has(nextPageUrl)) {
                            log.info(`Enqueuing next LIST page: ${nextPageUrl}`);
                            await requestQueue.addRequest({
                                url: nextPageUrl,
                                userData: {
                                    label: 'LIST',
                                    pageNo: pageNo + 1,
                                },
                            });
                        } else if (nextPageUrl) {
                            log.debug(
                                `Next LIST page already visited or queued: ${nextPageUrl}`
                            );
                        } else {
                            log.info(
                                `No next LIST page found from ${listUrl} (pageNo=${pageNo})`
                            );
                        }
                    } else if (pageNo >= MAX_PAGES) {
                        log.info(
                            `Reached MAX_PAGES=${MAX_PAGES}, not following pagination from ${listUrl}.`
                        );
                    }
                } else if (label === 'DETAIL') {
                    detailPages++;

                    if (!collectDetails) {
                        log.debug(
                            `DETAIL reached but collectDetails=false, skipping: ${request.url}`
                        );
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
                        const descriptionHtml = getDescriptionHtml($page);

                        // Prefer JSON-LD description, then HTML container description
                        const descriptionText =
                            cleanText(jsonLd?.descriptionHtml) ||
                            cleanText(descriptionHtml);

                        // Date posted: JSON-LD, then DOM/text heuristics
                        const datePostedLd = jsonLd?.datePosted || null;
                        const datePostedText = getDatePosted($page);
                        const datePosted = datePostedLd || datePostedText || null;

                        const finalTitle = jsonLd?.title || titleHtml || null;
                        const finalCompany = jsonLd?.company || companyHtml || null;
                        const finalLocation = jsonLd?.location || locationHtml || null;

                        const employmentType = getEmploymentType($page, jsonLd, descriptionText);
                        const salary = getSalary($page, jsonLd, descriptionText);

                        const record = {
                            url: detailUrl,
                            title: finalTitle,
                            company: finalCompany,
                            location: finalLocation,
                            datePosted,
                            employmentType: employmentType || null,
                            validThrough: jsonLd?.validThrough || null,
                            salary: salary || null,
                            descriptionHtml: jsonLd?.descriptionHtml || descriptionHtml || null,
                            descriptionText: descriptionText || null,
                            language: pageLang || null,
                            source: 'rekrute.com',
                            scrapedAt: new Date().toISOString(),
                        };

                        await Dataset.pushData(record);
                        saved++;

                        log.info(
                            `Saved DETAIL ${saved}/${RESULTS_WANTED}: ${detailUrl}`
                        );
                    } catch (err) {
                        log.error(
                            `Failed to process DETAIL ${request.url}: ${err.message}`
                        );
                        if (session) session.retire();
                    }
                } else {
                    log.warning(
                        `Unknown label "${label}" for URL ${request.url}, skipping.`
                    );
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
    // eslint-disable-next-line no-console
    console.error(err);
    process.exit(1);
});
