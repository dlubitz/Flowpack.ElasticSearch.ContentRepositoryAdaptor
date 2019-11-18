<?php
declare(strict_types=1);

namespace Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentRepositoryAdaptor package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Domain\Model\TargetContextPath;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\DocumentDriverInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\IndexDriverInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\IndexerDriverInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\NodeTypeMappingBuilderInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\RequestDriverInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Driver\SystemDriverInterface;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\ElasticSearchClient;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Exception;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\Error\BulkIndexingError;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Indexer\Error\MalformedBulkRequestError;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Service\DimensionsService;
use Flowpack\ElasticSearch\ContentRepositoryAdaptor\Service\ErrorHandlingService;
use Flowpack\ElasticSearch\Domain\Model\Document as ElasticSearchDocument;
use Flowpack\ElasticSearch\Domain\Model\Index;
use Flowpack\ElasticSearch\Transfer\Exception\ApiException;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\ContentRepository\Domain\Service\ContentDimensionCombinator;
use Neos\ContentRepository\Domain\Service\Context;
use Neos\ContentRepository\Domain\Service\ContextFactory;
use Neos\ContentRepository\Search\Indexer\AbstractNodeIndexer;
use Neos\ContentRepository\Search\Indexer\BulkNodeIndexerInterface;
use Neos\ContentRepository\Utility;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Persistence\Exception\IllegalObjectTypeException;
use Neos\Utility\Files;
use Neos\Flow\Log\Utility\LogEnvironment;
use Psr\Log\LoggerInterface;

/**
 * Indexer for Content Repository Nodes. Triggered from the NodeIndexingManager.
 *
 * Internally, uses a bulk request.
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexer extends AbstractNodeIndexer implements BulkNodeIndexerInterface
{
    /**
     * @Flow\Inject
     * @var NodeTypeMappingBuilderInterface
     */
    protected $nodeTypeMappingBuilder;

    /**
     * Optional postfix for the index, e.g. to have different indexes by timestamp.
     *
     * @var string
     */
    protected $indexNamePostfix = '';

    /**
     * @Flow\Inject
     * @var ErrorHandlingService
     */
    protected $errorHandlingService;

    /**
     * @Flow\Inject
     * @var ElasticSearchClient
     */
    protected $searchClient;

    /**
     * @Flow\Inject
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @Flow\Inject
     * @var ContentDimensionCombinator
     */
    protected $contentDimensionCombinator;

    /**
     * @Flow\Inject
     * @var ContextFactory
     */
    protected $contextFactory;

    /**
     * @var DocumentDriverInterface
     * @Flow\Inject
     */
    protected $documentDriver;

    /**
     * @var IndexerDriverInterface
     * @Flow\Inject
     */
    protected $indexerDriver;

    /**
     * @var IndexDriverInterface
     * @Flow\Inject
     */
    protected $indexDriver;

    /**
     * @var RequestDriverInterface
     * @Flow\Inject
     */
    protected $requestDriver;

    /**
     * @var SystemDriverInterface
     * @Flow\Inject
     */
    protected $systemDriver;

    /**
     * @var array
     * @Flow\InjectConfiguration(package="Flowpack.ElasticSearch.ContentRepositoryAdaptor", path="indexing.batchSize")
     */
    protected $batchSize;

    /**
     * The current Elasticsearch bulk request, in the format required by http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-bulk.html
     *
     * @var array
     */
    protected $currentBulkRequest = [];

    /**
     * @var boolean
     */
    protected $bulkProcessing = false;

    /**
     * @var DimensionsService
     * @Flow\Inject
     */
    protected $dimensionService;

    public function setDimensions(array $dimensionsValues): void
    {
        $this->searchClient->setDimensions($dimensionsValues);
    }

    /**
     * Returns the index name to be used for indexing, with optional indexNamePostfix appended.
     *
     * @return string
     * @throws Exception
     */
    public function getIndexName(): string
    {
        $indexName = $this->searchClient->getIndexName();
        if ($this->indexNamePostfix !== '') {
            $indexName .= '-' . $this->indexNamePostfix;
        }

        return $indexName;
    }

    /**
     * Set the postfix for the index name
     *
     * @param string $indexNamePostfix
     * @return void
     */
    public function setIndexNamePostfix(string $indexNamePostfix): void
    {
        $this->indexNamePostfix = $indexNamePostfix;
    }

    /**
     * Return the currently active index to be used for indexing
     *
     * @return Index
     * @throws Exception
     * @throws \Flowpack\ElasticSearch\Exception
     */
    public function getIndex(): Index
    {
        $index = $this->searchClient->findIndex($this->getIndexName());
        $index->setSettingsKey($this->searchClient->getIndexName());

        return $index;
    }

    /**
     * Index this node, and add it to the current bulk request.
     *
     * @param NodeInterface $node
     * @param string $targetWorkspaceName In case indexing is triggered during publishing, a target workspace name will be passed in
     * @return void
     */
    public function indexNode(NodeInterface $node, $targetWorkspaceName = null): void
    {
        $indexer = function (NodeInterface $node, $targetWorkspaceName = null) {
            $contextPath = $node->getContextPath();

            if ($this->settings['indexAllWorkspaces'] === false) {
                // we are only supposed to index the live workspace.
                // We need to check the workspace at two occasions; checking the
                // $targetWorkspaceName and the workspace name of the node's context as fallback
                if ($targetWorkspaceName !== null && $targetWorkspaceName !== 'live') {
                    return;
                }

                if ($targetWorkspaceName === null && $node->getContext()->getWorkspaceName() !== 'live') {
                    return;
                }
            }

            if ($targetWorkspaceName !== null) {
                $contextPath = (string)(new TargetContextPath($node, $targetWorkspaceName, $contextPath));
            }

            $documentIdentifier = $this->calculateDocumentIdentifier($node, $targetWorkspaceName);
            $nodeType = $node->getNodeType();

            $mappingType = $this->getIndex()->findType($this->nodeTypeMappingBuilder->convertNodeTypeNameToMappingName($nodeType->getName()));

            if ($this->bulkProcessing === false) {
                // Remove document with the same contextPathHash but different NodeType, required after NodeType change
                $this->logger->debug(sprintf('NodeIndexer (%s): Search and remove duplicate document for node %s (%s) if needed.', $documentIdentifier, $contextPath, $node->getIdentifier()), LogEnvironment::fromMethodName(__METHOD__));
                $this->documentDriver->deleteDuplicateDocumentNotMatchingType($this->getIndex(), $documentIdentifier, $node->getNodeType());
            }

            $fulltextIndexOfNode = [];
            $nodePropertiesToBeStoredInIndex = $this->extractPropertiesAndFulltext($node, $fulltextIndexOfNode, function ($propertyName) use ($documentIdentifier, $node) {
                $this->logger->debug(sprintf('NodeIndexer (%s) - Property "%s" not indexed because no configuration found, node type %s.', $documentIdentifier, $propertyName, $node->getNodeType()->getName()), LogEnvironment::fromMethodName(__METHOD__));
            });

            $document = new ElasticSearchDocument($mappingType,
                $nodePropertiesToBeStoredInIndex,
                $documentIdentifier
            );

            $documentData = $document->getData();
            if ($targetWorkspaceName !== null) {
                $documentData['__workspace'] = $targetWorkspaceName;
            }

            if ($this->isFulltextEnabled($node)) {
                $this->toBulkRequest($node, $this->indexerDriver->document($this->getIndexName(), $node, $document, $documentData));
                $this->toBulkRequest($node, $this->indexerDriver->fulltext($node, $fulltextIndexOfNode, $targetWorkspaceName));
            }

            $this->logger->debug(sprintf('NodeIndexer (%s): Indexed node %s.', $documentIdentifier, $contextPath));
        };

        $handleNode = function (NodeInterface $node, Context $context) use ($targetWorkspaceName, $indexer) {
            $nodeFromContext = $context->getNodeByIdentifier($node->getIdentifier());
            if ($nodeFromContext instanceof NodeInterface) {
                $this->searchClient->withDimensions(function () use ($indexer, $nodeFromContext, $targetWorkspaceName) {
                    $indexer($nodeFromContext, $targetWorkspaceName);
                }, $nodeFromContext->getContext()->getTargetDimensions());
            } else {
                $documentIdentifier = $this->calculateDocumentIdentifier($node, $targetWorkspaceName);
                if ($node->isRemoved()) {
                    $this->removeNode($node, $context->getWorkspaceName());
                    $this->logger->debug(sprintf('NodeIndexer (%s): Removed node with identifier %s, no longer in workspace %s', $documentIdentifier, $node->getIdentifier(), $context->getWorkspaceName()), LogEnvironment::fromMethodName(__METHOD__));
                } else {
                    $this->logger->debug(sprintf('NodeIndexer (%s): Could not index node with identifier %s, not found in workspace %s with dimensions %s', $documentIdentifier, $node->getIdentifier(), $context->getWorkspaceName(), json_encode($context->getDimensions())), LogEnvironment::fromMethodName(__METHOD__));
                }
            }
        };

        $workspaceName = $targetWorkspaceName ?: $node->getContext()->getWorkspaceName();
        $dimensionCombinations = $this->contentDimensionCombinator->getAllAllowedCombinations();
        if ($dimensionCombinations !== []) {
            foreach ($dimensionCombinations as $combination) {
                $handleNode($node, $this->createContentContext($workspaceName, $combination));
            }
        } else {
            $handleNode($node, $this->createContentContext($workspaceName));
        }
    }

    protected function createContentContext(string $workspaceName, array $dimensions = []): Context
    {
        $configuration = [
            'workspaceName' => $workspaceName,
            'invisibleContentShown' => true
        ];
        if ($dimensions !== []) {
            $configuration['dimensions'] = $dimensions;
        }
        return $this->contextFactory->create($configuration);
    }

    protected function toBulkRequest(NodeInterface $node, array $tuple = null)
    {
        if ($tuple === null) {
            return;
        }

        $this->currentBulkRequest[] = new BulkRequestPart($this->dimensionService->hashByNode($node), $tuple);
        $this->flushIfNeeded();
    }

    /**
     * Returns a stable identifier for the Elasticsearch document representing the node
     *
     * @param NodeInterface $node
     * @param string $targetWorkspaceName
     * @return string
     * @throws IllegalObjectTypeException
     */
    protected function calculateDocumentIdentifier(NodeInterface $node, $targetWorkspaceName = null): string
    {
        $contextPath = $node->getContextPath();

        if ($targetWorkspaceName !== null) {
            $contextPath = (string)(new TargetContextPath($node, $targetWorkspaceName, $contextPath));
        }

        return sha1($contextPath);
    }

    /**
     * Schedule node removal into the current bulk request.
     *
     * @param NodeInterface $node
     * @param string $targetWorkspaceName
     * @return void
     * @throws IllegalObjectTypeException
     */
    public function removeNode(NodeInterface $node, string $targetWorkspaceName = null): void
    {
        if ($this->settings['indexAllWorkspaces'] === false) {
            // we are only supposed to index the live workspace.
            // We need to check the workspace at two occasions; checking the
            // $targetWorkspaceName and the workspace name of the node's context as fallback
            if ($targetWorkspaceName !== null && $targetWorkspaceName !== 'live') {
                return;
            }

            if ($targetWorkspaceName === null && $node->getContext()->getWorkspaceName() !== 'live') {
                return;
            }
        }

        $documentIdentifier = $this->calculateDocumentIdentifier($node, $targetWorkspaceName);

        $this->toBulkRequest($node, $this->documentDriver->delete($node, $documentIdentifier));
        $this->toBulkRequest($node, $this->indexerDriver->fulltext($node, [], $targetWorkspaceName));

        $this->logger->debug(sprintf('NodeIndexer (%s): Removed node %s (%s) from index.', $documentIdentifier, $node->getContextPath(), $node->getIdentifier()), LogEnvironment::fromMethodName(__METHOD__));
    }

    protected function flushIfNeeded(): void
    {
        if ($this->bulkRequestLenght() >= $this->batchSize['elements'] || $this->bulkRequestSize() >= $this->batchSize['octets']) {
            $this->flush();
        }
    }

    protected function bulkRequestSize(): int
    {
        return array_reduce($this->currentBulkRequest, function ($sum, BulkRequestPart $request) {
            return $sum + $request->getSize();
        }, 0);
    }

    protected function bulkRequestLenght(): int
    {
        return count($this->currentBulkRequest);
    }

    /**
     * Perform the current bulk request
     *
     * @return void
     * @throws Exception
     * @throws \Flowpack\ElasticSearch\Exception
     * @throws \Neos\Utility\Exception\FilesException
     */
    public function flush(): void
    {
        $bulkRequest = $this->currentBulkRequest;
        $bulkRequestSize = $this->bulkRequestLenght();
        if ($bulkRequestSize === 0) {
            return;
        }

        $this->logger->debug(vsprintf('Flush bulk request, elements=%d, maximumElements=%s, octets=%d, maximumOctets=%d',
            [$bulkRequestSize, $this->batchSize['elements'], $this->bulkRequestSize(), $this->batchSize['octets']]),
            LogEnvironment::fromMethodName(__METHOD__));

        $payload = [];
        /** @var BulkRequestPart $bulkRequestPart */
        foreach ($bulkRequest as $bulkRequestPart) {
            if (!$bulkRequestPart instanceof BulkRequestPart) {
                throw new \RuntimeException('Invalid bulk request part');
            }

            $hash = $bulkRequestPart->getTargetDimensionsHash();

            if (!isset($payload[$hash])) {
                $payload[$hash] = [];
            }

            foreach ($bulkRequestPart->getRequest() as $bulkRequestItem) {
                if ($bulkRequestItem === null) {
                    $this->errorHandlingService->log(
                        new MalformedBulkRequestError('Indexing Error: Bulk request item could not be encoded as JSON - ' . json_last_error_msg(), $bulkRequestItem)
                    );
                    continue 2;
                }
                $payload[$hash][] = $bulkRequestItem;
            }
        }

        if ($payload === []) {
            $this->reset();
            return;
        }

        $logDirectory = FLOW_PATH_DATA . 'Logs/ElasticSearch/';
        if (!@is_dir($logDirectory)) {
            Files::createDirectoryRecursively($logDirectory);
        }

        // TODO: Remove fileystem logging
        foreach ($this->dimensionService->getDimensionsRegistry() as $hash => $dimensions) {
            if (!isset($payload[$hash])) {
                continue;
            }
            $this->searchClient->setDimensions($dimensions);
            $response = $this->requestDriver->bulk($this->getIndex(), implode(chr(10), $payload[$hash]));
            foreach ($response as $responseLine) {
                if (isset($response['errors']) && $response['errors'] !== false) {
                    $this->errorHandlingService->log(
                        new BulkIndexingError($this->currentBulkRequest, $responseLine)
                    );
                    file_put_contents($logDirectory . 'BulkIndexing_Error_' . time() . '.json', json_encode([
                        'request' => $payload[$hash],
                        'response' => $responseLine
                    ], JSON_PRETTY_PRINT));
                }
            }
        }


        $this->reset();
    }

    protected function reset()
    {
        $this->dimensionService->reset();
        $this->currentBulkRequest = [];
    }

    /**
     * Update the index alias
     *
     * @return void
     * @throws Exception
     * @throws ApiException
     * @throws \Flowpack\ElasticSearch\Exception
     */
    public function updateIndexAlias(): void
    {
        $aliasName = $this->searchClient->getIndexName(); // The alias name is the unprefixed index name
        if ($this->getIndexName() === $aliasName) {
            throw new Exception('UpdateIndexAlias is only allowed to be called when $this->setIndexNamePostfix has been created.', 1383649061);
        }

        if (!$this->getIndex()->exists()) {
            throw new Exception('The target index for updateIndexAlias does not exist. This shall never happen.', 1383649125);
        }

        $aliasActions = [];
        try {
            $indexNames = $this->indexDriver->indexesByAlias($aliasName);
            if ($indexNames === []) {
                // if there is an actual index with the name we want to use as alias, remove it now
                $this->indexDriver->deleteIndex($aliasName);
            } else {
                foreach ($indexNames as $indexName) {
                    $aliasActions[] = [
                        'remove' => [
                            'index' => $indexName,
                            'alias' => $aliasName
                        ]
                    ];
                }
            }
        } catch (ApiException $exception) {
            // in case of 404, do not throw an error...
            if ($exception->getResponse()->getStatusCode() !== 404) {
                throw $exception;
            }
        }

        $aliasActions[] = [
            'add' => [
                'index' => $this->getIndexName(),
                'alias' => $aliasName
            ]
        ];

        $this->indexDriver->aliasActions($aliasActions);
    }

    /**
     * Update the main alias to allow to query all indices at once
     */
    public function updateMainAlias()
    {
        $aliasActions = [];
        $aliasNamePrefix = $this->searchClient->getIndexNamePrefix(); // The alias name is the unprefixed index name

        $indexNames = $this->indexDriver->indexesByPrefix($aliasNamePrefix);
        $indexNames = \array_values(\array_filter($indexNames, function ($indexName) {
            $suffix = '-' . $this->indexNamePostfix;
            $indexNameParts = \explode('-', $indexName);
            return substr($indexName, 0 - strlen($suffix)) === $suffix && count($indexNameParts) === 3;
        }));

        $cleanupAlias = function ($alias) use (&$aliasActions) {
            try {
                $indexNames = $this->indexDriver->indexesByAlias($alias);
                if ($indexNames === []) {
                    // if there is an actual index with the name we want to use as alias, remove it now
                    $this->indexDriver->deleteIndex($alias);
                } else {
                    foreach ($indexNames as $indexName) {
                        $aliasActions[] = [
                            'remove' => [
                                'index' => $indexName,
                                'alias' => $alias
                            ]
                        ];
                    }
                }
            } catch (ApiException $exception) {
                // in case of 404, do not throw an error...
                if ($exception->getResponse()->getStatusCode() !== 404) {
                    throw $exception;
                }
            }
        };

        $postfix = function ($alias) {
            return $alias . '-' . $this->indexNamePostfix;
        };

        if (\count($indexNames) > 0) {
            $cleanupAlias($aliasNamePrefix);
            $cleanupAlias($postfix($aliasNamePrefix));

            foreach ($indexNames as $indexName) {
                $aliasActions[] = [
                    'add' => [
                        'index' => $indexName,
                        'alias' => $aliasNamePrefix
                    ]
                ];
                $aliasActions[] = [
                    'add' => [
                        'index' => $indexName,
                        'alias' => $postfix($aliasNamePrefix)
                    ]
                ];
            }
        }

        $this->indexDriver->aliasActions($aliasActions);
    }

    /**
     * Remove old indices which are not active anymore (remember, each bulk index creates a new index from scratch,
     * making the "old" index a stale one).
     *
     * @return array<string> a list of index names which were removed
     * @throws Exception
     */
    public function removeOldIndices(): array
    {
        $aliasName = $this->searchClient->getIndexName(); // The alias name is the unprefixed index name

        $currentlyLiveIndices = $this->indexDriver->indexesByAlias($aliasName);

        $indexStatus = $this->systemDriver->status();
        $allIndices = array_keys($indexStatus['indices']);

        $indicesToBeRemoved = [];

        foreach ($allIndices as $indexName) {
            if (strpos($indexName, $aliasName . '-') !== 0) {
                // filter out all indices not starting with the alias-name, as they are unrelated to our application
                continue;
            }

            if (in_array($indexName, $currentlyLiveIndices, true)) {
                // skip the currently live index names from deletion
                continue;
            }

            $indicesToBeRemoved[] = $indexName;
        }

        array_map(function ($index) {
            $this->indexDriver->deleteIndex($index);
        }, $indicesToBeRemoved);

        return $indicesToBeRemoved;
    }

    /**
     * Perform indexing without checking about duplication document
     *
     * This is used during bulk indexing to improve performance
     *
     * @param callable $callback
     * @throws \Exception
     */
    public function withBulkProcessing(callable $callback)
    {
        $bulkProcessing = $this->bulkProcessing;
        $this->bulkProcessing = true;
        try {
            /** @noinspection PhpUndefinedMethodInspection */
            $callback->__invoke();
        } catch (\Exception $exception) {
            $this->bulkProcessing = $bulkProcessing;
            throw $exception;
        }
        $this->bulkProcessing = $bulkProcessing;
    }
}
